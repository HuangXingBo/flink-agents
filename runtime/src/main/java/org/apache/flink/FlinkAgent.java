/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.feedback.FeedbackKey;
import org.apache.flink.message.Message;
import org.apache.flink.message.MessageTypeInformation;
import org.apache.flink.operator.FeedbackOperatorFactory;
import org.apache.flink.operator.FeedbackSinkOperator;
import org.apache.flink.operator.FunctionGroupFactory;
import org.apache.flink.operator.IngressRouterOperator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.function.SerializableFunction;

/** h. */
public class FlinkAgent {

    private final StreamExecutionEnvironment env;

    public FlinkAgent(StreamExecutionEnvironment env) {
        this.env = env;
    }

    public static DataStream<?> fromDataStream(DataStream<Row> dataStream) {
        MessageTypeInformation typeInfo = new MessageTypeInformation();
        DataStream<Message> in =
                dataStream
                        .transform("ingress-router", typeInfo, new IngressRouterOperator())
                        .setParallelism(dataStream.getParallelism())
                        //                        .uid("ingress-router")
                        .returns(typeInfo);
        //        SingleOutputStreamOperator<String> out = in.map(message -> (String)
        // message.target().id());
        //
        FeedbackKey<Message> feedbackKey = new FeedbackKey<>("statefun-pipeline", 1L);
        DataStream<Message> feedbackOperator =
                in.keyBy((KeySelector<Message, Object>) message -> message.target().id())
                        .transform(
                                "feedback",
                                typeInfo,
                                new FeedbackOperatorFactory(
                                        feedbackKey, new FeedbackKeySelector()));
        //                        .uid("feedback");
        //

        OutputTag<Integer> outputTag = new OutputTag<>("feedback-output", Types.INT);
        SingleOutputStreamOperator<Message> functionGroupOperator =
                DataStreamUtils.reinterpretAsKeyedStream(
                                feedbackOperator,
                                (KeySelector<Message, Object>) message -> message.target().id())
                        .transform("function-group", typeInfo, new FunctionGroupFactory(outputTag));
        //                        .uid("function-group");

        FeedbackSinkOperator sinkOperator = new FeedbackSinkOperator(feedbackKey);

        DataStream<Void> feedbackSink =
                functionGroupOperator
                        .keyBy((KeySelector<Message, Object>) message -> message.target().id())
                        .transform("feedback_sink", TypeInformation.of(Void.class), sinkOperator);
        //                        .uid("feedback_sink");

        String stringKey = feedbackKey.asColocationKey();
        feedbackOperator.getTransformation().setCoLocationGroupKey(stringKey);
        functionGroupOperator.getTransformation().setCoLocationGroupKey(stringKey);
        feedbackSink.getTransformation().setCoLocationGroupKey(stringKey);

        feedbackOperator.getTransformation().setParallelism(functionGroupOperator.getParallelism());

        feedbackSink.getTransformation().setParallelism(functionGroupOperator.getParallelism());

        //        return out;
        return functionGroupOperator.getSideOutput(outputTag);
    }

    private static final class FeedbackKeySelector
            implements SerializableFunction<Message, Object> {

        private static final long serialVersionUID = 1;

        @Override
        public Object apply(Message message) {
            return message.target().id();
        }
    }
}
