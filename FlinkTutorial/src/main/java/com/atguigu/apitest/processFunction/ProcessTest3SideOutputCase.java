package com.atguigu.apitest.processFunction;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author hao.peng01@hand-china.com 2020/12/17 16:18
 */
public class ProcessTest3SideOutputCase {
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

        //定义一个OutputTag，用来表示侧输出流低温度
        OutputTag<SensorReading> lowTempTag = new OutputTag<SensorReading>("lowTemp") {

        };

        //测试ProcessFunction,自定义侧输出流实现分流操作
        SingleOutputStreamOperator<SensorReading> highTempStream = dataStream.process(new ProcessFunction<SensorReading, SensorReading>() {

            @Override
            public void processElement(SensorReading value, Context context, Collector<SensorReading> out) throws Exception {
                //判断温度，大于30度，高温流输出到主流，小于低温流输出到侧输出流
                if (value.getTemperature() > 30) {
                    out.collect(value);
                } else {
                    context.output(lowTempTag, value);
                }
            }
        });
        highTempStream.print("high-temp");
        highTempStream.getSideOutput(lowTempTag).print("low-temp");

        env.execute();
    }

}
