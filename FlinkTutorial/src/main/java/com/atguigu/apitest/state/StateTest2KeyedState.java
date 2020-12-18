package com.atguigu.apitest.state;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author hao.peng01@hand-china.com 2020/12/17 16:18
 */
public class StateTest2KeyedState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //从文件中读取数据
//        DataStream<String> inputStream = env.readTextFile("D:\\WorkSpace\\GitDepository\\Code\\Flink\\FlinkTutorial\\src\\main\\resources\\sensor.txt");
        //socket 文本流
        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);
        //转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });
        //定义一个有状态的map操作，统计当前分区数据个数
        SingleOutputStreamOperator<Integer> resultStream = dataStream
                .keyBy("id")
                .map(new MyKeyCountMapper());
        resultStream.print();
        env.execute();
    }

    public static class MyKeyCountMapper extends RichMapFunction<SensorReading, Integer> {
        private ValueState<Integer> keyCountState;
        //其他类型的声明
        private ListState<String> myListState;
        private MapState<String, Double> myMapState;
        private ReducingState<SensorReading> myReducingState;

        @Override
        public void open(Configuration parameters) throws Exception {
            keyCountState = getRuntimeContext().getState(new ValueStateDescriptor<>("key-count", Integer.class));

            myListState = getRuntimeContext().getListState(new ListStateDescriptor<String>("my-list", String.class));

            myMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Double>("my-map", String.class, Double.class));

//            myReducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<SensorReading>());
        }

        @Override
        public Integer map(SensorReading sensorReading) throws Exception {

            //其他状态API调用
            for (String str : myListState.get()) {
                System.out.println(str);
            }
            Iterable<String> strings = myListState.get();
            myListState.add("hello");
            //map state
            myMapState.get("1");
            myMapState.put("2", 12.3);
            myMapState.entries();
            myMapState.contains("2");

            myReducingState.add(sensorReading);
            myReducingState.clear();

            Integer count;
            if (keyCountState.value() == null) {
                keyCountState.update(0);
            }
            count = keyCountState.value();
            count++;
            keyCountState.update(count);
            return count;


        }
    }
}
