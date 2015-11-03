package fi.aalto.dmg;

import com.google.common.base.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by jun on 11/3/15.
 */
public class StreamingWordCount {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Stateful Network Word Count");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(1));
        ssc.checkpoint("checkpoint");

        JavaReceiverInputDStream<String> lines = ssc.socketTextStream("127.0.0.1", 9999);
        JavaPairDStream<String, Long> wordCounts = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String l) throws Exception {
                return Arrays.asList(l.split(" "));
            }
        }).mapToPair(new PairFunction<String, String, Long>() {
            public Tuple2<String, Long> call(String w) throws Exception {
                return new Tuple2<>(w, 1L);
            }
        }).groupByKey()
                .window(Durations.seconds(5), Durations.seconds(5))
                .mapToPair(new PairFunction<Tuple2<String, Iterable<Long>>, String, Long>() {
                    @Override
                    public Tuple2<String, Long> call(Tuple2<String, Iterable<Long>> stringIterableTuple2) throws Exception {
                        Long sum = 0L;
                        for(Long l : stringIterableTuple2._2()){
                            sum += l;
                        }
                        return new Tuple2<>(stringIterableTuple2._1(), sum);
                    }
                })
                .updateStateByKey(new Function2<List<Long>, Optional<Long>, Optional<Long>>() {
                    public Optional<Long> call(List<Long> values, Optional<Long> state) throws Exception {
                        if (values == null || values.isEmpty()) {
                            return state;
                        }
                        long sum = 0L;
                        for (Long v : values) {
                            sum += v;
                        }
                        return Optional.of(state.or(0L) + sum);
                    }
                });
//                .updateStateByKey(new Function2<List<Iterable<Long>>, Optional<Long>, Optional<Long>>() {
//                    @Override
//                    public Optional<Long> call(List<Iterable<Long>> iterables, Optional<Long> longOptional) throws Exception {
//                        if (iterables == null || iterables.isEmpty()) {
//                            return longOptional;
//                        }
//                        long sum = 0L;
//                        for (Iterable<Long> iterable : iterables) {
//                            for(Long l : iterable)
//                                sum += l;
//                        }
//                        return Optional.of(longOptional.or(0L) + sum);
//                    }
//                });

        wordCounts.print();

        ssc.start();
        ssc.awaitTermination();
    }
}
