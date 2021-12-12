import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class F01_SenToKafka {
    public static void main(String[] args) throws Exception{
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String kafkaHostPort =parameterTool.get("kafkaHostPort");
        String kafkaTopic = parameterTool.get("kafkaTopic");

        Logger logger = LoggerFactory.getLogger(FlinkDemo.class);
        logger.trace("trace");
        logger.debug("debug");
        logger.info("info");
        logger.warn("warn");
        logger.error("error");

        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从集合中读取数据
        DataStream<String> dataStream = env.fromCollection(Arrays.asList(
                "{\"aa\":1, \"bb\":2}",
                "{\"aa\":3, \"bb\":4}",
                "{\"aa\":5, \"bb\":6}"
        ));

        //DataStream<Integer> integerDataStream = env.fromElements(1, 2, 4, 67, 189);

        // 打印输出
        dataStream.print("data");
        //integerDataStream.print("int");
        dataStream.addSink( new FlinkKafkaProducer011<String>(kafkaHostPort, kafkaTopic, new SimpleStringSchema()));
        // 执行
        env.execute();
    }
}
