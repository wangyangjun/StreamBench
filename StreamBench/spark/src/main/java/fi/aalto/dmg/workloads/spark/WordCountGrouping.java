package fi.aalto.dmg.workloads.spark;

import fi.aalto.dmg.exceptions.WorkloadException;
import fi.aalto.dmg.workloads.WordCountWorkload;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;
import com.google.common.base.Optional;

import java.util.*;

/**
 * Created by yangjun.wang on 20/10/15.
 */
public class WordCountGrouping extends WordCountWorkload {
    public WordCountGrouping() throws WorkloadException {
        super();
    }

    @Override
    public void Process() throws WorkloadException {
        System.out.println("Spark WordCountGrouping");

        SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("spark://headnodehost:7077");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        Map<String, Integer> topicMap = new HashMap<String, Integer>();
        topicMap.put(this.getTopic(), this.getSourceNum());

        JavaPairReceiverInputDStream kafkaStream =
                KafkaUtils.createStream(jssc, this.kafkaSource.getKafkaZookeeperConnect(), this.getClass().getSimpleName(), topicMap);

        JavaDStream<String> words = kafkaStream.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String s) {
                return Arrays.asList(s.toLowerCase().split("\\W+"));
            }
        });

        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        JavaPairDStream<String, Iterable<Integer>> groupedPairs = pairs.groupByKey();

        JavaPairDStream<String, Integer> wordCounts = groupedPairs.mapToPair(new PairFunction<Tuple2<String, Iterable<Integer>>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Iterable<Integer>> stringIterableTuple2) throws Exception {
                int count = 0;
                for(Integer a : stringIterableTuple2._2()){
                    count += a;
                }
                return new Tuple2<String, Integer>(stringIterableTuple2._1(), count);
            }
        });

        JavaPairDStream<String, Integer> cumulateCounts = wordCounts.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
            @Override
            public Optional<Integer> call(List<Integer> values, Optional<Integer> count) throws Exception {
                Integer newSum = count.or(0);
                for(Integer value : values){
                    newSum += value;
                }
                return Optional.of(newSum);
            }
        });
        cumulateCounts.print();
        jssc.start();
        jssc.awaitTermination();
    }
}
