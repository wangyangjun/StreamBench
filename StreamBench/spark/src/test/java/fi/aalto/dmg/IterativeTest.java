package fi.aalto.dmg;

import com.google.common.base.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.util.SystemClock;
import scala.Tuple2;

import java.util.*;

/**
 * Created by jun on 23/11/15.
 */
public class IterativeTest {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setMaster("local[2]")
                .setAppName("Stateful Network Word Count")
                .set("spark.driver.allowMultipleContexts", "true");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(1));

        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> rdd = sc.parallelize(data);
        LinkedList<JavaRDD<Integer>> rddQueue = new LinkedList<>();
        rddQueue.add(rdd);

        JavaDStream<Integer> numbers = ssc.queueStream(rddQueue);

        JavaDStream<Integer> minusOne = numbers.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) throws Exception {
                return integer - 1;
            }
        });
        JavaDStream<Integer> stillGraterThanZero = minusOne.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer integer) throws Exception {
                return integer > 0;
            }
        });

        minusOne = minusOne.union(stillGraterThanZero);


        minusOne.print();

        ssc.start();
        ssc.awaitTermination();
    }
}
