package com.atguigu.chapter06;

import com.atguigu.chapter05.ClickSource;
import com.atguigu.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

public class WindowProcessTest {
        public static void main(String[] args) throws Exception {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(4);
            env.getConfig().setAutoWatermarkInterval(100);

            SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                    //有序流的watermark生成
                    .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                            //乱序流的watermark生成
                            //.assignTimestampsAndWatermarks( WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                            .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                @Override
                                public long extractTimestamp(Event event, long l) {
                                    return event.timestamp;
                                }
                            }));

            // 使用ProcessWindowFunction 计算UV
            stream.keyBy(data -> true)
                            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                                    .process(new UvCountByWindow())
                                            .print();

            env.execute();
        }

        // 实现自定义的ProcessWindowFunction，输出一条统计信息
        public static class UvCountByWindow extends ProcessWindowFunction<Event, String, Boolean, TimeWindow>{
            @Override
            public void process(Boolean aBoolean, ProcessWindowFunction<Event, String, Boolean, TimeWindow>.Context context, Iterable<Event> iterable, Collector<String> collector) throws Exception {
                // 用一个HashSet保存user
                HashSet<String> userSet = new HashSet<>();
                // 从elements中遍历数据，放到set中去重
                for (Event event: iterable){
                    userSet.add(event.user);
                }

                Integer uv = userSet.size();
                // 结合窗口信息
                Long start = context.window().getStart();
                Long end = context.window().getEnd();
                collector.collect("窗口 "+ new Timestamp(start) + "~" + new Timestamp(end)
                + "UV值为：" + uv);

            }
        }

}
