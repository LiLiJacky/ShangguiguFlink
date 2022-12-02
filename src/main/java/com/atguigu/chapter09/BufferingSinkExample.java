package com.atguigu.chapter09;

import com.atguigu.chapter05.ClickSource;
import com.atguigu.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class BufferingSinkExample {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                             }
                        }));

        stream.print("input");

        // 批量缓存输出
        stream.addSink(new BufferingSink(10));

        env.execute();
    }

    // 自定义实现SinkFunction
    public static class BufferingSink implements SinkFunction<Event>, CheckpointedFunction{
        // 定义当前类的属性，批量
        private final int threshold;

        public BufferingSink(int threshold) {
            this.threshold = threshold;
            this.bufferedElements = new ArrayList<>();
        }

        private List<Event> bufferedElements;

        // 定义一个算子状态
        private ListState<Event> checkpointedState;

        @Override
        public void invoke(Event value, Context context) throws Exception {
            bufferedElements.add(value); //缓存到列表
            // 判断如果达到阈值，就批量写入
            if (bufferedElements.size() == threshold) {
                // 用打印到控制台模拟写入外部系统
                for (Event element: bufferedElements)
                    System.out.println(element);

                System.out.println("=====================输出完毕================");
                bufferedElements.clear();
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
            // 清空状态
            checkpointedState.clear();

            // 对状态进行持久化，复制缓存的列表到列表状态
            for (Event element: bufferedElements)
                checkpointedState.add(element);
        }

        @Override
        public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
            // 定义算子状态
            ListStateDescriptor<Event> des = new ListStateDescriptor<>("buffered-elements", Event.class);

            checkpointedState = functionInitializationContext.getOperatorStateStore().getListState(des);

            // 如果从故障恢复，需要将ListState中所有的元素复制到列表中
            if (functionInitializationContext.isRestored()) {
                for (Event element: checkpointedState.get())
                    bufferedElements.add(element);
            }
        }
    }

}
