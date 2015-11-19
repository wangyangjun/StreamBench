package fi.aalto.dmg;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.*;

/**
 * Spark streaming join test
 * Two streams:
 * Names: Yangjun Jun Junshao Wang Hello World
 * Name-age:
 *      Yangjun 5
 *      Jun 14
 *      Junshao 29
 *      Hello 23
 *      Hi 23
 * Created by jun on 18/11/15.
 */
public class SparkJoinTest {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("SparkJoinTest");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(1));
        ssc.checkpoint("checkpoint");

        JavaReceiverInputDStream<String> lines = ssc.socketTextStream("127.0.0.1", 9999);
        JavaPairDStream<String, Long> nameStream = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String l) throws Exception {
                return Arrays.asList(l.split(" "));
            }
        }).mapToPair(new PairFunction<String, String, Long>() {
            public Tuple2<String, Long> call(String w) throws Exception {
                return new Tuple2<>(w, 1L);
            }
        }).window(Durations.seconds(15));

        JavaReceiverInputDStream<String> lines2 = ssc.socketTextStream("127.0.0.1", 9998);
        JavaPairDStream<String, Long> nameAgeStream = lines2.mapToPair(new PairFunction<String, String, Long>() {
            @Override
            public Tuple2<String, Long> call(String s) throws Exception {
                String[] list = s.split(" ");
                String name  = list[0];
                long age = 0L;
                if(list.length>1)
                    age = Long.parseLong(list[1]);
                return new Tuple2<String, Long>(name, age);
            }
        }).window(Durations.seconds(10));

        nameStream.print();
        nameAgeStream.print();
        JavaPairDStream<String, Tuple2<Long, Long>> joinedStream = nameStream.join(nameAgeStream);
        joinedStream.print();

        ssc.start();
        ssc.awaitTermination();
    }
}
