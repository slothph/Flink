package com.atguigu.apitest.sink;

import com.atguigu.apitest.beans.SensorReading;
import com.atguigu.apitest.source.SourceTest4UDF;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author hao.peng01@hand-china.com 2020/12/16 11:37
 */
public class SinkTest3JDBC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //从文件中读取数据
//        DataStream<String> inputStream = env.readTextFile("D:\\WorkSpace\\GitDepository\\Code\\Flink\\FlinkTutorial\\src\\main\\resources\\sensor.txt");
//        DataStream<SensorReading> dataStream = inputStream.map(line -> {
//            String[] fields = line.split(",");
//            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
//        });

        DataStreamSource<SensorReading> dataStream = env.addSource(new SourceTest4UDF.MySensorSource());
        dataStream.addSink(new MyJDBCSink());


        env.execute();


    }

    //实现自定义的SinkFunction
    private static class MyJDBCSink extends RichSinkFunction<SensorReading> {
        Connection connection = null;
        PreparedStatement insertStmt = null;
        PreparedStatement updateStmt = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "19990709");
            insertStmt = connection.prepareStatement("insert into sensor_temp(id,temp)values (?,?)");
            updateStmt = connection.prepareStatement("insert sensor_temp set temp = ? where id = ?");
        }

        //每来一条数据，调用链接，执行sql
        @Override
        public void invoke(SensorReading value, Context context) throws Exception {
            //直接执行更新语句，如果没有更新那么就插入
            updateStmt.setDouble(1, value.getTemperature());
            updateStmt.setString(2, value.getId());
            updateStmt.execute();
            if (updateStmt.getUpdateCount() == 0) {
                insertStmt.setString(1, value.getId());
                insertStmt.setDouble(2, value.getTemperature());
                updateStmt.execute();
            }

        }

        @Override
        public void close() throws Exception {
            insertStmt.close();
            updateStmt.close();
            connection.close();
        }
    }
}
