import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.nio.charset.StandardCharsets;


public class Main {
    public static void main(String[] args) throws Exception {
        org.apache.flink.streaming.api.environment.StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromElements("hello","world")
                .addSink(new RichSinkFunction<String>() {
                    public org.apache.hadoop.conf.Configuration configuration;
                    public org.apache.hadoop.hbase.client.Connection connection;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        configuration = HBaseConfiguration.create();
                        configuration.set("hbase.zookeeper.quorum","hadoop102");
                        connection = ConnectionFactory.createConnection(configuration);
                    }

                    @Override
                    public void invoke(String value, Context context) throws Exception {
                        Table table = connection.getTable(TableName.valueOf("test"));
                        Put put = new Put("rowkey".getBytes(StandardCharsets.UTF_8));
                        put.addColumn("info".getBytes(StandardCharsets.UTF_8)
                                , value.getBytes(StandardCharsets.UTF_8)
                                , "123".getBytes(StandardCharsets.UTF_8)
                        );
                        table.put(put);
                        table.close();
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                        connection.close();
                    }
                });
        env.execute();
    }
}
