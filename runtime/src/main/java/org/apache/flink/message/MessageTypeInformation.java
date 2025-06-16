package org.apache.flink.message;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;

public class MessageTypeInformation extends TypeInformation<Message> {

    public MessageTypeInformation() {}

    @Override
    public boolean isBasicType() {
        return false;
    }

    @Override
    public boolean isTupleType() {
        return false;
    }

    @Override
    public int getArity() {
        return 0;
    }

    @Override
    public int getTotalFields() {
        return 0;
    }

    @Override
    public Class<Message> getTypeClass() {
        return Message.class;
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    @Override
    public TypeSerializer<Message> createSerializer(ExecutionConfig executionConfig) {
        return new KryoSerializer<>(Message.class, executionConfig);
    }

    @Override
    public String toString() {
        return "MessageTypeInformation";
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof MessageTypeInformation;
    }

    @Override
    public int hashCode() {
        return Message.class.hashCode();
    }

    @Override
    public boolean canEqual(Object o) {
        return o instanceof MessageTypeInformation;
    }
}
