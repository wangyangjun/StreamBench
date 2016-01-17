package fi.aalto.dmg;

import com.google.common.collect.Lists;
import fi.aalto.dmg.exceptions.WorkloadException;
import fi.aalto.dmg.workloads.WordCount;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Hello world!
 *
 */
public class App 
{
    private static final Pattern SPACE = Pattern.compile(" ");
    public static void main( String[] args ) throws ClassNotFoundException, WorkloadException, InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException, IOException {

        System.out.println("Start...");
        String[] testArgs = {"ClickedAdvertisement"}; // WordCount WordCountWindowed ClickedAdvertisement
        BenchStarter.main(testArgs);

    }
}
