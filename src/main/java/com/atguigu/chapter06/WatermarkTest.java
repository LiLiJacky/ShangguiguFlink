package com.atguigu.chapter06;

import com.atguigu.chapter05.Event;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class WatermarkTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.getConfig().setAutoWatermarkInterval(100);

        //  从元素读取数据
        DataStream<Event> stream = env.fromElements(
                new Event("mary", "./home", 1000L),
                new Event("Alice", "./prob?id=100", 2000L),
                new Event("Bob", "./cart", 3000L),
                new Event("Bob", "./prob?id=1", 4000L),
                new Event("Bob", "./home", 4500L))
//                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
//                                    @Override
//                                    public long extractTimestamp(Event event, long l) {
//                                        return event.timestamp;
//                                    }
//                                })
//                        )
                //乱序流的watermark生成
                .assignTimestampsAndWatermarks( WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));

        env.execute();
    }
}
