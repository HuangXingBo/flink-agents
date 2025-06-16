package org.apache.flink.operator;

import org.apache.flink.message.Message;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.util.OutputTag;

public class FunctionGroupFactory
        implements OneInputStreamOperatorFactory<Message, Message> {
    private final OutputTag<Integer> outputTag;

    public FunctionGroupFactory(OutputTag<Integer> outputTag) {
        this.outputTag = outputTag;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends StreamOperator<Message>> T createStreamOperator(
            StreamOperatorParameters<Message> streamOperatorParameters) {
        FunctionGroupOperator fn =
                new FunctionGroupOperator(
                        outputTag,
                        streamOperatorParameters.getMailboxExecutor(),
                        ChainingStrategy.ALWAYS,
                        streamOperatorParameters.getProcessingTimeService());
        fn.setup(
                streamOperatorParameters.getContainingTask(),
                streamOperatorParameters.getStreamConfig(),
                streamOperatorParameters.getOutput());

        return (T) fn;
    }

    @Override
    public void setChainingStrategy(ChainingStrategy chainingStrategy) {}

    @Override
    public ChainingStrategy getChainingStrategy() {
        return ChainingStrategy.ALWAYS;
    }

    @Override
    public Class<? extends StreamOperator<?>> getStreamOperatorClass(ClassLoader classLoader) {
        return FunctionGroupOperator.class;
    }
}
