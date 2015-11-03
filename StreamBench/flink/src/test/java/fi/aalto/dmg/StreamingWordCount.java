package fi.aalto.dmg;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.WindowMapFunction;
import org.apache.flink.streaming.api.windowing.StreamWindow;
import org.apache.flink.streaming.api.windowing.helper.Time;
import org.apache.flink.util.Collector;


import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * Created by jun on 11/3/15.
 */
public class StreamingWordCount {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final DataStream<Tuple2<String, Integer>> counts = env
                .socketTextStream("localhost", 9999)
                .flatMap(new Splitter())
                .groupBy(0)
                .window(Time.of(5, TimeUnit.SECONDS))
                .reduceWindow(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t0, Tuple2<String, Integer> t1) throws Exception {
                        return new Tuple2<String, Integer>(t0.f0, t0.f1+t1.f1);
                    }
                })
                .flatten();

        counts.print();
        // counts.groupBy(0).sum(1).print();
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
