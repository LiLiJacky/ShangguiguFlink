package com.atguigu.chapter05;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformSimpleAggTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //  从元素读取数据
        DataStreamSource<Event> stream = env.fromElements(
                new Event("mary", "./home", 1000L),
                new Event("Alice", "./prob?id=100", 2000L),
                new Event("Bob", "./cart", 3000L),
                new Event("Bob", "./prob?id=1", 4000L),
                new Event("Bob", "./home", 4500L)
        );

        //  按键分组之后进行聚合，提取当前用户最近一次访问数据
        stream.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event event) throws Exception {
                return event.user;
            }
        }).max("timestamp")
                .print("max:");

        stream.keyBy(data -> data.user)
                        .maxBy("timestamp")
                                .print("maxBy:");

        env.execute();
    }
}
