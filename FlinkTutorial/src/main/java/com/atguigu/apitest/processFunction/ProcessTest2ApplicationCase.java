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
public class ProcessTest2ApplicationCase {
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
                .process(new TempConIncreWarning(10)).print();

        env.execute();
    }

    //实现自定义处理函数，检测一段时间内的温度连续上升，输出报警
    public static class TempConIncreWarning extends KeyedProcessFunction<Tuple, SensorReading, String> {
        //定义私有属性，当前统计额时间间隔
        private Integer interval;

        //定义状态，保存上一次的温度值，定时器时间戳
        private ValueState<Double> lastTempState;
        private ValueState<Long> timerTsState;

        public TempConIncreWarning(Integer interval) {
            this.interval = interval;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<>("last-temp", Double.class, Double.MIN_VALUE));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<>("timer-ts", Long.class));
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //定时器触发,输出报警信息
            out.collect("传感器" + ctx.getCurrentKey().getField(0) + "温度值连续" + interval + "s上升");
            timerTsState.clear();
        }

        @Override
        public void processElement(SensorReading value, Context context, Collector<String> collector) throws Exception {
            //取出状态
            Double lastTemp = lastTempState.value();
            Long timerTs = timerTsState.value();
            //如果温度上升并且无定时器时，注册10秒后的定时器，开始等待
            if (value.getTemperature() > lastTemp && timerTs == null) {
                //计算出定时器时间戳
                Long ts = context.timerService().currentProcessingTime() + interval * 1000L;
                context.timerService().registerProcessingTimeTimer(ts);
                timerTsState.update(ts);
            } //如果温度下降，那么删除定时器
            else if (value.getTemperature() < lastTemp && timerTs != null) {
                context.timerService().deleteProcessingTimeTimer(timerTs);
                timerTsState.clear();
            }
            //更新温度状态
            lastTempState.update(value.getTemperature());
        }

        @Override
        public void close() throws Exception {
            lastTempState.clear();
        }

    }
}
