package com.atguigu.chapter05;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TransformFlatMapTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //  从元素读取数据
        DataStreamSource<Event> stream = env.fromElements(
                new Event("mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Jack", "./cart", 3000L)
        );

        // 1. 实现FlatMapFunction
        stream.flatMap(new MyFlatMap()).print("1");

        // 2. 传入Lambda表达式
        stream.flatMap((Event event, Collector<String> out) -> {
            if (event.user.equals("mary")){
                out.collect(event.user);
            }else if (event.user.equals("Bob")){
                out.collect(event.user);
                out.collect(event.url);
            }
        }).returns(new TypeHint<String>() {
        }).print("lambda click:");

        env.execute();
    }

    // 实现一个自定义的FlatMapFunction
    public static class MyFlatMap implements FlatMapFunction<Event, String>{
        @Override
        public void flatMap(Event event, Collector<String> collector) throws Exception {
            collector.collect(event.user);
            collector.collect(event.url);
            collector.collect(event.timestamp.toString());
        }
    }
}
