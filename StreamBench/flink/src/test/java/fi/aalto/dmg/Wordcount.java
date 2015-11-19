package fi.aalto.dmg;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * Created by jun on 11/5/15.
 */
public class Wordcount {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final WindowedStream<Tuple2<String, Integer>, String, TimeWindow> counts = env
                .socketTextStream("localhost", 9999)
                .flatMap(new Splitter())
                .keyBy(new KeySelector<Tuple2<String,Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                })
                .timeWindow(Time.of(5, TimeUnit.SECONDS));

        final WindowedStream<Tuple2<String, Integer>, String, TimeWindow> counts2 = env
                .socketTextStream("localhost", 9998)
                .flatMap(new Splitter())
                .keyBy(new KeySelector<Tuple2<String,Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                })
                .timeWindow(Time.of(5, TimeUnit.SECONDS));

        env.execute("Socket Stream WordCount");
    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word: sentence.split(" ")) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
