package org.apache.flink.operator;

import org.apache.flink.message.Address;
import org.apache.flink.message.DataMessage;
import org.apache.flink.message.FunctionType;
import org.apache.flink.message.Message;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.Row;

/** h. */
public class IngressRouterOperator extends AbstractStreamOperator<Message>
        implements OneInputStreamOperator<Row, Message> {

    private transient StreamRecord<Message> reuse;

    @Override
    public void open() throws Exception {
        super.open();
        reuse = new StreamRecord<>(null);
    }

    @Override
    public void processElement(StreamRecord<Row> streamRecord) {
        System.out.println("IngressRouterOperator " + streamRecord);
        Row value = streamRecord.getValue();
        Object id = value.getField(0);
        Object payload = value.getField(1);
        // TODO: change namespace and type
        Address target = new Address(new FunctionType("ingress", "router"), id);
        Message message = new DataMessage(null, target, payload);
        output.collect(reuse.replace(message));
    }
}
