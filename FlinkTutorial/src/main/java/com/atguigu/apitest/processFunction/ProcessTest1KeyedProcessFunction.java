package com.atguigu.apitest.processFunction;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author hao.peng01@hand-china.com 2020/12/17 16:18
 */
public class ProcessTest1KeyedProcessFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //socket 文本流
        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);
        //转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        //测试keyedProcessFunction,先分组然后自定义处理
        dataStream.keyBy("id")
                .process(new MyProcess()).print();


        env.execute();
    }

    //实现自定义函数类
    public static class MyProcess extends KeyedProcessFunction<Tuple, SensorReading, Integer> {


        ValueState<Long> tsTimerState;

        @Override
        public void open(Configuration parameters) throws Exception {
            tsTimerState = getRuntimeContext().getState(new ValueStateDescriptor<>("ts-timer", Long.class));
        }

        @Override
        public void processElement(SensorReading value, Context context, Collector<Integer> collector) throws Exception {
            collector.collect(value.getId().length());

            //context
            context.timestamp();
            context.getCurrentKey();
//            context.output();

            context.timerService().currentProcessingTime();
            context.timerService().currentWatermark();
            context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime() + 1000L);
            tsTimerState.update(context.timerService().currentProcessingTime() + 1000L);
//            context.timerService().registerEventTimeTimer((value.getTimestamp() + 10) * 1000L);
//            context.timerService().deleteProcessingTimeTimer(tsTimerState.value());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Integer> out) throws Exception {
            System.out.println(timestamp + " 定时器触发");
            ctx.getCurrentKey();
//            ctx.output();
            ctx.timeDomain();
        }

        @Override
        public void close() throws Exception {
            tsTimerState.clear();
        }
    }
}
