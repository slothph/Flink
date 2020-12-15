package com.atguigu.apitest.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * @author hao.peng01@hand-china.com 2020/12/14 19:56
 */
public class SourceTest3Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");

        //从文件中读取数据
        DataStreamSource<String> dataStream = env.addSource(new FlinkKafkaConsumer011<String>("sensor", new SimpleStringSchema(), properties));

        //打印输出
        dataStream.print();
        env.execute();
    }

}
