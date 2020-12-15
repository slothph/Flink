package com.atguigu.apitest.source;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Properties;
import java.util.Random;

/**
 * @author hao.peng01@hand-china.com 2020/12/14 20:31
 */
public class SourceTest4UDF {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");

        //从文件中读取数据
        DataStreamSource<SensorReading> dataStream = env.addSource(new MySensorSource());

        //打印输出
        dataStream.print();
        env.execute();
    }

    //实现自定义的SourceFunction
    public static class MySensorSource implements SourceFunction<SensorReading> {
        //定义一个标识位，用来控制数据的产生
        private boolean running = true;

        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            //定义一个随机数发生器
            Random random = new Random();
            //设置10个传感器的初始温度
            HashMap<String, Double> sensorTempMap = new HashMap<>();
            for (int i = 0; i < 10; i++) {
                sensorTempMap.put("sensor_" + (i + 1), 60 + random.nextGaussian() * 20);
            }
            while (running) {
                for (String sensorId : sensorTempMap.keySet()) {
                    //在当前温度基础上随机波动
                    Double newTemp = sensorTempMap.get(sensorId) + random.nextGaussian();
                    sensorTempMap.put(sensorId, newTemp);
                    ctx.collect(new SensorReading(sensorId, System.currentTimeMillis(), newTemp));
                }
                //控制输出频率
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
