package org.apache.flink.operator;

import org.apache.flink.feedback.FeedbackChannel;
import org.apache.flink.feedback.FeedbackChannelBroker;
import org.apache.flink.feedback.FeedbackKey;
import org.apache.flink.feedback.SubtaskFeedbackKey;
import org.apache.flink.message.CheckpointMessage;
import org.apache.flink.message.Message;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.IOUtils;

import java.util.Objects;

public class FeedbackSinkOperator extends AbstractStreamOperator<Void>
        implements OneInputStreamOperator<Message, Void> {

    private static final long serialVersionUID = 1;
    private final FeedbackKey<Message> key;
    private transient FeedbackChannel<Message> channel;

    public FeedbackSinkOperator(FeedbackKey<Message> key) {
        this.key = Objects.requireNonNull(key);
    }

    @Override
    public void processElement(StreamRecord<Message> streamRecord) {
        Message value = streamRecord.getValue();
        System.out.println("FeedbackSinkOperator processElement " + value);
        channel.put(value);
    }

    @Override
    public void open() throws Exception {
        super.open();
        final int indexOfThisSubtask = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        final int attemptNum = getRuntimeContext().getTaskInfo().getAttemptNumber();
        final SubtaskFeedbackKey<Message> key =
                this.key.withSubTaskIndex(indexOfThisSubtask, attemptNum);

        FeedbackChannelBroker broker = FeedbackChannelBroker.get();
        this.channel = broker.getChannel(key);
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        super.prepareSnapshotPreBarrier(checkpointId);
        Message sentinel = new CheckpointMessage(checkpointId);
        channel.put(sentinel);
    }

    @Override
    public void close() throws Exception {
        IOUtils.closeQuietly(channel);
        super.close();
    }
}
