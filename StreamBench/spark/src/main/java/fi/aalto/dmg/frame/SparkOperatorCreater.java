package fi.aalto.dmg.frame;

import fi.aalto.dmg.statistics.PerformanceStreamingListener;
import fi.aalto.dmg.util.Constant;
import fi.aalto.dmg.util.Point;
import fi.aalto.dmg.util.WithTime;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * Created by yangjun.wang on 25/10/15.
 */
public class SparkOperatorCreater extends OperatorCreator implements Serializable {

    private static final long serialVersionUID = 6964020640199730722L;
    public JavaStreamingContext jssc;
    private Properties properties;

    private static Function<Tuple2<String, String>, WithTime<String>> mapFunctionWithTime
            = new Function<Tuple2<String, String>, WithTime<String>>() {
        @Override
        public WithTime<String> call(Tuple2<String, String> stringStringTuple2) throws Exception {
            String[] list = stringStringTuple2._2().split(Constant.TimeSeparatorRegex);
            if(list.length == 2) {
                return new WithTime<String>(list[0], Long.parseLong(list[1]));
            }
            return new WithTime<>(stringStringTuple2._2(), System.currentTimeMillis());
        }
    };

    private static Function<Tuple2<String, String>, String> mapFunction
            = new Function<Tuple2<String, String>, String>() {
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
        conf.set("spark.streaming.ui.retainedBatches", "2000");
        jssc = new JavaStreamingContext(conf, Durations.milliseconds(this.getDurationsMilliseconds()));
    }

    @Override
    public SparkWorkloadOperator<WithTime<String>> stringStreamFromKafkaWithTime(String zkConStr,
                                                                                 String kafkaServers,
                                                                                 String group,
                                                                                 String topics,
                                                                                 String offset,
                                                                                 String componentId,
                                                                                 int parallelism) {
        HashSet<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        HashMap<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", kafkaServers);
        kafkaParams.put("auto.offset.reset", offset);
        kafkaParams.put("zookeeper.connect", zkConStr);
        kafkaParams.put("group.id", group);

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

        JavaDStream<WithTime<String>> lines = messages.map(mapFunctionWithTime);

        return new SparkWorkloadOperator<>(lines, parallelism);
    }

    @Override
    public WorkloadOperator<Point> pointStreamFromKafka(String zkConStr, String kafkaServers, String group, String topics, String offset, String componentId, int parallelism) {
        return null;
    }

    @Override
    public WorkloadOperator<String> stringStreamFromKafka(String zkConStr,
                                                          String kafkaServers,
                                                          String group,
                                                          String topics,
                                                          String offset,
                                                          String componentId,
                                                          int parallelism) {
        HashSet<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        HashMap<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", kafkaServers);
        kafkaParams.put("auto.offset.reset", offset);
        kafkaParams.put("zookeeper.connect", zkConStr);
        kafkaParams.put("group.id", group);

        // Create direct kafka stream with brokers and topics
        JavaPairDStream<String, String> messages = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topicsSet
        );

        JavaDStream<String> lines = messages.map(mapFunction);

        return new SparkWorkloadOperator<>(lines, parallelism);
    }


    @Override
    public void Start() {
        jssc.addStreamingListener(new PerformanceStreamingListener());

//        jssc.checkpoint("/tmp/log-analyzer-streaming");
//        jssc.checkpoint("hdfs://master:8020/usr/warehouse/wordcount/checkpoint");
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

