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

import java.util.*;

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
        })
                .window(Durations.seconds(5), Durations.seconds(5))
                .mapPartitions(new FlatMapFunction<Iterator<Tuple2<String,Long>>, Tuple2<String, Long>>() {
                    @Override
                    public Iterable<Tuple2<String, Long>> call(Iterator<Tuple2<String, Long>> tuple2Iterator) throws Exception {
                        Map<String, Tuple2<String, Long>> map = new HashMap<>();
                        while(tuple2Iterator.hasNext()){
                            Tuple2<String, Long> tuple2 = tuple2Iterator.next();
                            String word = tuple2._1();
                            Tuple2<String, Long> count = map.get(word);
                            if (count == null){
                                map.put(word, tuple2);
                            } else {
                                map.put(word, new Tuple2<>(word, count._2() + tuple2._2()));
                            }
                        }
                        return map.values();
                    }
                })
                .mapToPair(new PairFunction<Tuple2<String, Long>, String, Long>() {
                    @Override
                    public Tuple2<String, Long> call(Tuple2<String, Long> stringLongTuple2) throws Exception {
                        return stringLongTuple2;
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
