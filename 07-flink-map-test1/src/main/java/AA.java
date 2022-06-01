import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AA {
    public static void main(String[] args) throws Exception {
        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 读取文本流
        DataStreamSource<String> lineDSS = env.readTextFile("input/words.txt");
        SingleOutputStreamOperator<List<String>> map = lineDSS.map(
                new MapFunction<String, List<String>>() {
                    @Override
                    public List<String> map(String s) throws Exception {
                        // 1
//                        String[] words = s.split(" ");
//                        List<String> wds = new ArrayList<>();
//                        wds.addAll(Arrays.asList(words));
//                        return wds;
                        // 2
//                        String[] words = s.split(" ");
//                        List<String> wds = new ArrayList<>(Arrays.asList(words));
//                        return wds;
                        // 3
                        String[] words = s.split(" ");
                        return new ArrayList<>(Arrays.asList(words));
                    }
                }
        );
        map.print();

        env.execute();
    }
}
