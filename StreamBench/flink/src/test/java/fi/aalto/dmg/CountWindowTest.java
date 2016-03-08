package fi.aalto.dmg;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * Created by jun on 08/03/16.
 */
public class CountWindowTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Integer[] array = new Integer[]{1, 2, 4, 3, 4, 3, 4, 5, 4, 6, 7, 3, 3, 6, 1, 1, 3, 2, 4, 6};
        List<Integer> list = Arrays.asList(array);
        DataStream<Tuple2<Integer, Integer>> counts = env.fromCollection(list)
                .windowAll(GlobalWindows.create())
                .trigger(CountTrigger.of(5)).apply(new AllWindowFunction<Integer, Tuple2<Integer, Integer>, GlobalWindow>() {
                    @Override
                    public void apply(GlobalWindow window, Iterable<Integer> tuples, Collector<Tuple2<Integer, Integer>> out) throws Exception {
                        HashMap<Integer, Integer> map = new HashMap<>();
                        for(Integer tuple : tuples){
                            Integer value = 0;
                            if(map.containsKey(tuple)){
                                value = map.get(tuple);
                            }
                            map.put(tuple, value+1);
                        }

                        for(Map.Entry<Integer, Integer> entry : map.entrySet()) {
                            out.collect(new Tuple2<>(entry.getKey(), entry.getValue()));
                        }
                    }
                });

        counts.print();

        env.execute("Stream WordCount");
    }
}
