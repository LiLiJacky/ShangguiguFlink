package com.atguigu.chapter05;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SinkToFileTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


    }
}
