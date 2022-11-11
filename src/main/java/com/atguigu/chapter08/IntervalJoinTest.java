package com.atguigu.chapter08;

import com.atguigu.chapter05.ClickSource;
import com.atguigu.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class IntervalJoinTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 来自app的支付日志
        SingleOutputStreamOperator<Tuple3<String, String, Long>> orderStream = env.fromElements(
                Tuple3.of("Mary", "app", 1000L),
                Tuple3.of("Bob", "app", 2000L),
                Tuple3.of("Alice", "app", 1000L),
                Tuple3.of("Bob", "app", 2000L),
                Tuple3.of("Bob", "app", 1000L),
                Tuple3.of("Bob", "app", 2000L),
                Tuple3.of("Bob", "app", 1000L),
                Tuple3.of("Mary", "app", 2000L),
                Tuple3.of("Alice", "app", 3500L),
                Tuple3.of("Bob", "app", 21000L),
                Tuple3.of("Alice", "app", 12000L),
                Tuple3.of("Mary", "app", 23000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> stringStringLongTuple3, long l) {
                        return stringStringLongTuple3.f2;
                    }
                }));

        SingleOutputStreamOperator<Event> clickStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.timestamp;
                    }
                }));

        orderStream.keyBy(data -> data.f0)
                        .intervalJoin(clickStream.keyBy(data -> data.user))
                                .between(Time.seconds(-5),Time.seconds(10))
                                        .process(new ProcessJoinFunction<Tuple3<String, String, Long>, Event, String>() {
                                            @Override
                                            public void processElement(Tuple3<String, String, Long> stringStringLongTuple3, Event event, ProcessJoinFunction<Tuple3<String, String, Long>, Event, String>.Context context, Collector<String> collector) throws Exception {
                                                collector.collect(event + "=>" + stringStringLongTuple3);
                                            }
                                        }).print();

        env.execute();
    }
    }
