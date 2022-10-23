package com.atguigu.chapter05;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class TransformPartitionTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //  从元素读取数据
        DataStreamSource<Event> stream = env.fromElements(
                new Event("mary", "./home", 1000L),
                new Event("Alice", "./prob?id=100", 2000L),
                new Event("Bob", "./cart", 3000L),
                new Event("Bob", "./prob?id=1", 4000L),
                new Event("Bob", "./home", 4500L),
                new Event("Alice", "./cart", 3200L),
                new Event("Alice", "./prob?id=1", 4060L),
                new Event("Alice", "./home", 4650L)
        );

//        // 1. 随即分区
//        stream.shuffle().print().setParallelism(4);

        // 2, 轮询分区
//        stream.rebalance().print().setParallelism(4);

        // 3. rescal重缩放分区
        env.addSource(new RichParallelSourceFunction<Integer>() {
            @Override
            public void run(SourceContext<Integer> sourceContext) throws Exception {
                for (int i = 0; i < 8; i++){
                    // 将奇偶数分别发送到0号和1号并行分区
                    if (i % 2 == getRuntimeContext().getIndexOfThisSubtask())
                        sourceContext.collect(i);
                }
            }

            @Override
            public void cancel() {

            }
        }).setParallelism(2)
//                .rescale()
//                .print()
                .setParallelism(4);

        // 4. 广播
//        stream.broadcast().print().setParallelism(4);

        // 5. 全局分区
//        stream.global().print().setParallelism(4);

        // 6. 自定义重分区
        env.fromElements(1,2,3,4,5,6,7,8)
                        .partitionCustom(new Partitioner<Integer>() {
                            @Override
                            public int partition(Integer integer, int i) {
                                return integer % 2;
                            }
                        }, new KeySelector<Integer, Integer>() {
                            @Override
                            public Integer getKey(Integer integer) throws Exception {
                                return integer;
                            }
                        })
                                .print().setParallelism(4);

        env.execute();
    }
}
