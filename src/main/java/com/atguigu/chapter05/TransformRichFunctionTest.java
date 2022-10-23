package com.atguigu.chapter05;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformRichFunctionTest {
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

        stream.map(new MyRichMapper()).setParallelism(2)
                .print();

        env.execute();
    }

    // 实现一个自定义的富函数类
    public static class MyRichMapper extends RichMapFunction<Event, Integer>{
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            System.out.println("open生命周期被调用" + getRuntimeContext().getIndexOfThisSubtask() + "号任务启动");
        }

        @Override
        public Integer map(Event event) throws Exception {
            return event.url.length();
        }

        @Override
        public void close() throws Exception {
            super.close();
            System.out.println("open生命周期被调用" + getRuntimeContext().getIndexOfThisSubtask() + "号任务结束");
        }
    }
}
