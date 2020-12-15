package com.atguigu.apitest.sink;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.util.Properties;

/**
 * @author hao.peng01@hand-china.com 2020/12/15 17:30
 */
public class SinkTest1Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //从文件中读取数据
//        DataStreamSource<String> inputStream = env.readTextFile("D:\\WorkSpace\\GitDepository\\Code\\Flink\\FlinkTutorial\\src\\main\\resources\\sensor.txt");
        //从kafka读取数据
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");

        //从文件中读取数据
        DataStreamSource<String> inputStream = env.addSource(new FlinkKafkaConsumer011<String>("sensor", new SimpleStringSchema(), properties));


        DataStream<String> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2])).toString();
        });
        //写入
        dataStream.addSink(new FlinkKafkaProducer011<>("localhost:9092", "sinkTest", new SimpleStringSchema()));


        //打印输出
        dataStream.print();
        env.execute();
    }
}
