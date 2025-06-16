package org.apache.flink.operator;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.feedback.Checkpoints;
import org.apache.flink.feedback.FeedbackChannel;
import org.apache.flink.feedback.FeedbackChannelBroker;
import org.apache.flink.feedback.FeedbackConsumer;
import org.apache.flink.feedback.FeedbackKey;
import org.apache.flink.feedback.SubtaskFeedbackKey;
import org.apache.flink.logger.Loggers;
import org.apache.flink.logger.UnboundedFeedbackLogger;
import org.apache.flink.logger.UnboundedFeedbackLoggerFactory;
import org.apache.flink.message.CheckpointMessage;
import org.apache.flink.message.Message;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.state.KeyGroupStatePartitionStreamProvider;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.function.SerializableFunction;
import org.apache.flink.common.MailboxExecutorFacade;

import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.Executor;

/** h. */
public class FeedbackOperator extends AbstractStreamOperator<Message>
        implements FeedbackConsumer<Message>, OneInputStreamOperator<Message, Message> {
    private static final long serialVersionUID = 1L;
    private final TypeSerializer<Message> elementSerializer;
    private final FeedbackKey<Message> feedbackKey;
    private final MailboxExecutor mailboxExecutor;
    private final SerializableFunction<Message, ?> keySelector;
    private final long totalMemoryUsedForFeedbackCheckpointing;
    private transient StreamRecord<Message> reusable;
    private transient Checkpoints<Message> checkpoints;

    private transient boolean closedOrDisposed;

    FeedbackOperator(
            FeedbackKey<Message> feedbackKey,
            SerializableFunction<Message, ?> keySelector,
            long totalMemoryUsedForFeedbackCheckpointing,
            TypeSerializer<Message> elementSerializer,
            MailboxExecutor mailboxExecutor,
            ProcessingTimeService processingTimeService) {
        this.feedbackKey = Objects.requireNonNull(feedbackKey);
        this.keySelector = Objects.requireNonNull(keySelector);
        this.totalMemoryUsedForFeedbackCheckpointing = totalMemoryUsedForFeedbackCheckpointing;
        this.elementSerializer = elementSerializer;
        this.mailboxExecutor = mailboxExecutor;
        this.processingTimeService = processingTimeService;
    }

    @Override
    public void processFeedback(Message element) {
        if (closedOrDisposed) {
            return;
        }
        System.out.println("FeedbackOperator processFeedback " + element);
        OptionalLong maybeCheckpoint = element.isBarrierMessage();
        if (maybeCheckpoint.isPresent()) {
            checkpoints.commitCheckpointsUntil(maybeCheckpoint.getAsLong());
        } else {
            sendDownstream(element);
            checkpoints.append(element);
        }
    }

    @Override
    public void processElement(StreamRecord<Message> streamRecord) {
        System.out.println("FeedbackOperator processElement " + streamRecord);
        sendDownstream(streamRecord.getValue());
    }

    @Override
    @SuppressWarnings("unchecked")
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);

        final IOManager ioManager = getContainingTask().getEnvironment().getIOManager();
        final int maxParallelism =
                getRuntimeContext().getTaskInfo().getMaxNumberOfParallelSubtasks();

        this.reusable = new StreamRecord<>(null);

        //
        // Initialize the unbounded feedback logger
        //
        UnboundedFeedbackLoggerFactory<Message> feedbackLoggerFactory =
                (UnboundedFeedbackLoggerFactory<Message>)
                        Loggers.unboundedSpillableLoggerFactory(
                                ioManager,
                                maxParallelism,
                                totalMemoryUsedForFeedbackCheckpointing,
                                elementSerializer,
                                keySelector);

        this.checkpoints = new Checkpoints<>(feedbackLoggerFactory::create);

        //
        // we first must reply previously check-pointed envelopes before we start
        // processing any new envelopes.
        //
        UnboundedFeedbackLogger<Message> logger = feedbackLoggerFactory.create();
        for (KeyGroupStatePartitionStreamProvider keyedStateInput :
                context.getRawKeyedStateInputs()) {
            logger.replyLoggedEnvelops(keyedStateInput.getStream(), this);
        }

        registerFeedbackConsumer(new MailboxExecutorFacade(mailboxExecutor, "Feedback Consumer"));
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        checkpoints.startLogging(
                context.getCheckpointId(), context.getRawKeyedOperatorStateOutput());
    }

    @Override
    protected boolean isUsingCustomRawKeyedState() {
        return true;
    }

    @Override
    public void close() throws Exception {
        closeInternally();
        super.close();
    }

    private void closeInternally() {
        IOUtils.closeQuietly(checkpoints);
        checkpoints = null;
        closedOrDisposed = true;
    }

    private void registerFeedbackConsumer(Executor mailboxExecutor) {
        final int indexOfThisSubtask = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        final int attemptNum = getRuntimeContext().getTaskInfo().getAttemptNumber();
        final SubtaskFeedbackKey<Message> key =
                feedbackKey.withSubTaskIndex(indexOfThisSubtask, attemptNum);
        FeedbackChannelBroker broker = FeedbackChannelBroker.get();
        FeedbackChannel<Message> channel = broker.getChannel(key);
        channel.registerConsumer(this, mailboxExecutor);
    }

    private void sendDownstream(Message element) {
        reusable.replace(element);
        output.collect(reusable);
    }
}
