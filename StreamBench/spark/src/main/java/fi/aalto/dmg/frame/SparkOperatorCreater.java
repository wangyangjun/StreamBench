package fi.aalto.dmg.frame;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * Created by yangjun.wang on 25/10/15.
 */
public class SparkOperatorCreater extends OperatorCreater implements Serializable {

    public JavaStreamingContext jssc;
    private Properties properties;

    private static Function<Tuple2<String, String>, String> mapFunction = new Function<Tuple2<String, String>, String>() {
        @Override
        public String call(Tuple2<String, String> stringStringTuple2) throws Exception {
            return stringStringTuple2._2();
        }
    };

    public SparkOperatorCreater(String appName) throws IOException {
        super(appName);
        properties = new Properties();
        properties.load(this.getClass().getClassLoader().getResourceAsStream("spark-cluster.properties"));
        SparkConf conf = new SparkConf().setMaster(this.getMaster()).setAppName(appName);
        jssc = new JavaStreamingContext(conf, Durations.milliseconds(this.getDurationsMilliseconds()));
    }

    @Override
    public SparkWorkloadOperator<String> createOperatorFromKafka(String zkConStr, String kafkaServers, String group, String topics, String offset) {

        Map<String, Integer> topicMap = new HashMap<>();

        String[] topicsList = topics.split(",");
        for (String topic: topicsList) {
            topicMap.put(topic, 1);
        }
        //JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, zkConStr, group, topicMap);

        HashSet<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        HashMap<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", kafkaServers);
        kafkaParams.put("auto.offset.reset", offset);

        // Create direct kafka stream with brokers and topics
        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topicsSet
        );

        JavaDStream<String> lines = messages.map(mapFunction);
        return new SparkWorkloadOperator<>(lines);
    }

    @Override
    public void Start() {
        jssc.checkpoint("/tmp/log-analyzer-streaming");
        jssc.start();
        jssc.awaitTermination();
    }

    public String getMaster(){
        return properties.getProperty("cluster.master");
    }

    public long getDurationsMilliseconds(){
        return Long.parseLong(properties.getProperty("streaming.durations.milliseconds"));
    }
}

