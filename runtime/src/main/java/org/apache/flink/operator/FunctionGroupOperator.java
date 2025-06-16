package org.apache.flink.operator;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.backpressure.ThresholdBackPressureValve;
import org.apache.flink.message.DataMessage;
import org.apache.flink.message.Message;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.OutputTag;
import pemja.core.PythonInterpreter;
import pemja.core.PythonInterpreterConfig;

import java.util.Objects;

public class FunctionGroupOperator extends AbstractStreamOperator<Message>
        implements OneInputStreamOperator<Message, Message> {

    private static final long serialVersionUID = 1L;
    private final OutputTag<Integer> outputTag;
    private final transient MailboxExecutor mailboxExecutor;

    private transient StreamRecord<Message> reused;
    private transient StreamRecord<Integer> reusedOutput;

    private transient ThresholdBackPressureValve backPressureValve;
    private transient PythonInterpreter interpreter;

    public FunctionGroupOperator(
            OutputTag<Integer> outputTag,
            MailboxExecutor mailboxExecutor,
            ChainingStrategy chainingStrategy,
            ProcessingTimeService processingTimeService) {
        this.outputTag = outputTag;
        this.mailboxExecutor = Objects.requireNonNull(mailboxExecutor);
        this.chainingStrategy = chainingStrategy;
        this.processingTimeService = processingTimeService;
    }

    @Override
    public void processElement(StreamRecord<Message> record) throws Exception {
        while (backPressureValve.shouldBackPressure()) {
            mailboxExecutor.yield();
        }
        System.out.println("FunctionGroupOperator processElement " + record);
        DataMessage value = (DataMessage) record.getValue();
        Object result = interpreter.invoke("test_hehe.add", value.payload());
        output.collect(outputTag, reusedOutput.replace(1));
        output.collect(reused.replace(new DataMessage(value.source(), value.target(), result)));
    }

    @Override
    public void open() throws Exception {
        super.open();

        Objects.requireNonNull(mailboxExecutor, "MailboxExecutor is unexpectedly NULL");

        reused = new StreamRecord<>(null);

        reusedOutput = new StreamRecord<>(null);

        this.backPressureValve = new ThresholdBackPressureValve(100);
        PythonInterpreterConfig config =
                PythonInterpreterConfig.newBuilder()
                        .setPythonExec(
                                "/Users/duanchen/opensource/flink/flink-python/dev/.conda/bin/python") // specify python exec, use "python" on Windows
                        .addPythonPaths("/Users/duanchen/source/flink-agent-demo")
                        .build();
        this.interpreter = new PythonInterpreter(config);
        interpreter.exec("import test_hehe");
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        // TODO: support async.
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
