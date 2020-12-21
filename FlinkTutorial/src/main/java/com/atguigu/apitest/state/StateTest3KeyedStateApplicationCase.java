package com.atguigu.apitest.state;

import akka.japi.tuple.Tuple3;
import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author hao.peng01@hand-china.com 2020/12/17 16:18
 */
public class StateTest3KeyedStateApplicationCase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);

        //检查点配置
        env.enableCheckpointing(300);
        //高级配置
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(100);
        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(0);

        //3.重启策略配置
        //固定延迟重启
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 100000L));
        //失败率重启
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.minutes(10), Time.minutes(1)));


        //socket 文本流
        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);
        //转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });
        //定义一个flatMap操作，检测温度跳变，输出报警
        SingleOutputStreamOperator<Tuple3<String, Double, Double>> resultStream = dataStream.keyBy("id")
                .flatMap(new TempChangeWarning(10.0));

        resultStream.print();
        env.execute();
    }

    //实现自定义函数类
    public static class TempChangeWarning extends RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>> {
        //私有属性，温度跳变阙值
        private Double threshold;

        public TempChangeWarning(double threshold) {
            this.threshold = threshold;
        }

        //定义一个状态，保存上一次的温度值
        private ValueState<Double> lastTempState;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp", Double.class));

        }

        @Override
        public void flatMap(SensorReading value, Collector<Tuple3<String, Double, Double>> out) throws Exception {
            //获取状态
            Double lastTemp = lastTempState.value();
            //如果状态不为null，那么久判断两次温度差值
            if (lastTemp != null) {
                Double diff = Math.abs(value.getTemperature() - lastTemp);
                if (diff >= threshold) {
                    out.collect(new Tuple3<>(value.getId(), lastTemp, value.getTemperature()));
                }
            }
            //更新状态
            lastTempState.update(value.getTemperature());
        }


        @Override
        public void close() throws Exception {
            lastTempState.clear();
        }
    }
}
