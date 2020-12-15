package com.atguigu.apitest.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author hao.peng01@hand-china.com 2020/12/14 17:53
 */
public class SourceTest2File {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //从文件中读取数据
        DataStreamSource<String> dataStream = env.readTextFile("D:\\WorkSpace\\GitDepository\\Code\\Flink\\FlinkTutorial\\src\\main\\resources\\sensor.txt");

        //打印输出
        dataStream.print();
        env.execute();
    }
}
