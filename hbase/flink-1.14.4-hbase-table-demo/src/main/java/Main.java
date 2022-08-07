import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class Main {

    public static void main(String[] args)  {

        // 创建表的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // 从kafka中读取数据
        String sourceTable = "CREATE TABLE source_table (\n" +
                "  bidtime string ,\n" +
                "  `price` double,\n" +
                "  `item` STRING,\n" +
                "  `supplier_id` STRING,\n" +
                "  `rt` as cast( bidtime as timestamp(3) ),\n" +
                "   watermark for rt as rt - interval '5' second\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'topn1',\n" +
                "  'properties.bootstrap.servers' = 'hadoop01:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'csv'\n" +
                ")";


        tenv.executeSql(sourceTable);
        // 10分钟滚动窗口中的交易金额最大的前2笔订单
        tenv.executeSql("select\n" +
                "  *\n" +
                "from(\n" +
                "  select window_start,window_end, \n" +
                "    bidtime,\n" +
                "    price,\n" +
                "    item,\n" +
                "    supplier_id,\n" +
                "    row_number() over(partition by window_start,window_end order by price desc ) as rn\n" +
                "  from table(\n" +
                "    tumble(table source_table,descriptor(rt),interval '10' minute)\n" +
                "  ) \n" +
                ") t1 where rn <= 2 ").print();
    }


}
