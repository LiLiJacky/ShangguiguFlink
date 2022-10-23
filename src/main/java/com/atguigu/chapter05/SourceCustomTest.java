package com.atguigu.chapter05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.kafka.common.protocol.types.Field;

import java.util.Random;

public class SourceCustomTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

//        DataStreamSource<Event> customStream = env.addSource(new ClickSource());
        DataStreamSource<Integer> customStream = env.addSource(new ParalleCustomSource()).setParallelism(2);

        customStream.print();

        env.execute();

    }

    public static class ParalleCustomSource implements ParallelSourceFunction<Integer>{
        private Boolean running = true;
        private Random random = new Random();

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            while (running){
                ctx.collect(random.nextInt());
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

}
