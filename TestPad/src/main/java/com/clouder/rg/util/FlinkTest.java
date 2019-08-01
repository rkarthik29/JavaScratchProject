package com.clouder.rg.util;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkTest {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> stream = env.readTextFile("/Users/knarayanan/Downloads/2019-07-01.txt")
                .map(new MapFunction<String, String>() {
                    private String[] tokens;

                    public String map(String value) throws Exception {
                        return value;
                    }
                });
        stream.print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
