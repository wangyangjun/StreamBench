package fi.aalto.dmg;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.WindowedDataStream;
import org.apache.flink.streaming.api.datastream.temporal.StreamJoinOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.helper.Time;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * Created by jun on 11/5/15.
 */
public class Wordcount {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final WindowedDataStream<Tuple2<String, Integer>> counts = env
                .socketTextStream("localhost", 9999)
                .flatMap(new Splitter())
                .groupBy(0)
                .window(Time.of(5, TimeUnit.SECONDS));

        final WindowedDataStream<Tuple2<String, Integer>> counts2 = env
                .socketTextStream("localhost", 9998)
                .flatMap(new Splitter())
                .groupBy(0)
                .window(Time.of(5, TimeUnit.SECONDS));


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
