package com.atguigu.chapter05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformReduceTest {
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
        
        // 1. 统计每个用户的访问频次
        SingleOutputStreamOperator<Tuple2<String, Long>> clicksByUser = stream.map(new MapFunction<Event, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Event event) throws Exception {
                        return Tuple2.of(event.user, 1L);
                    }
                }).keyBy(data -> data.f0)
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> stringLongTuple2, Tuple2<String, Long> t1) throws Exception {
                        return Tuple2.of(stringLongTuple2.f0, stringLongTuple2.f1 + t1.f1);
                    }
                });

        // 2.选取当前最活跃的用户
        SingleOutputStreamOperator<Tuple2<String, Long>> result = clicksByUser.keyBy(data -> "key")
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> stringLongTuple2, Tuple2<String, Long> t1) throws Exception {
                        return stringLongTuple2.f1 > t1.f1 ? stringLongTuple2 : t1;
                    }
                });

        result.print();

        env.execute();
    }
}
