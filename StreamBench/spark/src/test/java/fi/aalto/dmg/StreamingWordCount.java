package fi.aalto.dmg;

import com.google.common.base.Optional;
import fi.aalto.dmg.statistics.PerformanceStreamingListener;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.scheduler.StatsReportListener;
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

        ssc.addStreamingListener(new PerformanceStreamingListener());


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
                .reduceByKey(new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long aLong, Long aLong2) throws Exception {
                        return aLong+aLong2;
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
        wordCounts.foreach(new Function2<JavaPairRDD<String, Long>, Time, Void>() {
            @Override
            public Void call(JavaPairRDD<String, Long> stringLongJavaPairRDD, Time time) throws Exception {
                return null;
            }
        });
        ssc.start();
        ssc.awaitTermination();
    }
}
