package fi.aalto.dmg.frame;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by yangjun.wang on 25/10/15.
 */
public class SparkOperatorCreater extends OperatorCreater {

    private JavaStreamingContext jssc;
    private Properties properties;


    public SparkOperatorCreater(String appName) throws IOException {
        super(appName);
        properties = new Properties();
        properties.load(this.getClass().getClassLoader().getResourceAsStream("spark-cluster.properties"));
        SparkConf conf = new SparkConf().setMaster(this.getMaster()).setAppName(appName);
        jssc = new JavaStreamingContext(conf, Durations.milliseconds(this.getDurationsMilliseconds()));
    }

    @Override
    public SparkWorkloadOperator<String> createOperatorFromKafka(String zkConStr, String group, String topics) {

        Map<String, Integer> topicMap = new HashMap<String, Integer>();
        String[] topicsList = topics.split(",");
        for (String topic: topicsList) {
            topicMap.put(topic, 1);
        }
        JavaPairReceiverInputDStream<String, String> lines = KafkaUtils.createStream(jssc, zkConStr, group, topicMap);

        return new SparkWorkloadOperator<>(lines);
    }

    @Override
    public void Start() {
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

