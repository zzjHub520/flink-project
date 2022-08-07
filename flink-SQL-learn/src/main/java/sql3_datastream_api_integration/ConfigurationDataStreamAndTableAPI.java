package sql3_datastream_api_integration;

import java.time.ZoneId;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ConfigurationDataStreamAndTableAPI {
    public static void main(String[] args) {

// create Java DataStream API

//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//// set various configuration early
//
//        env.setMaxParallelism(256);
//
//        env.getConfig().addDefaultKryoSerializer(MyCustomType.class, CustomKryoSerializer.class);
//
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//
//// then switch to Java Table API
//
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//
//// set configuration early
//
//        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("Europe/Berlin"));
    }
}
