package fi.aalto.dmg.frame;

import fi.aalto.dmg.util.Constant;
import fi.aalto.dmg.util.WithTime;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer082;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by yangjun.wang on 25/10/15.
 */
public class FlinkOperatorCreator extends OperatorCreator {

    private static final long serialVersionUID = 4194701654519072721L;
    private Properties properties;
    final StreamExecutionEnvironment env;

    public FlinkOperatorCreator(String name) throws IOException {
        super(name);
//        properties = new Properties();
        env = StreamExecutionEnvironment.getExecutionEnvironment();
//        properties.load(this.getClass().getClassLoader().getResourceAsStream("flink-cluster.properties"));
    }

    @Override
    public WorkloadOperator<WithTime<String>> createOperatorFromKafkaWithTime(String zkConStr,
                                                                              String kafkaServers,
                                                                              String group,
                                                                              String topics,
                                                                              String offset,
                                                                              String componentId,
                                                                              int parallelism) {
        /*
        * Note that the Kafka source is expecting the following parameters to be set
        *  - "bootstrap.servers" (comma separated list of kafka brokers)
        *  - "zookeeper.connect" (comma separated list of zookeeper servers)
        *  - "group.id" the id of the consumer group
        *  - "topic" the name of the topic to read data from.
        *  "--bootstrap.servers host:port,host1:port1 --zookeeper.connect host:port --topic testTopic"
        */
        Properties properties = new Properties();
        properties.put("bootstrap.servers", kafkaServers);
        properties.put("zookeeper.connect", zkConStr);
        properties.put("group.id", group);
        properties.put("topic", topics);
        properties.put("auto.commit.enable", false);
        properties.put("auto.offset.reset", offset);

        env.setParallelism(parallelism);
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer082<String>(topics, new SimpleStringSchema(), properties));
        DataStream<WithTime<String>> withTimeDataStream = stream.map(new MapFunction<String, WithTime<String>>() {
            @Override
            public WithTime<String> map(String value) throws Exception {
                String[] list = value.split(Constant.TimeSeparatorRegex);
                if(list.length == 2) {
                    return new WithTime<String>(list[0], Long.parseLong(list[1]));
                }
                return new WithTime<String>(value, System.currentTimeMillis());
            }
        });
        return new FlinkWorkloadOperator<>(withTimeDataStream);
    }

    @Override
    public WorkloadOperator<String> createOperatorFromKafka(String zkConStr,
                                                            String kafkaServers,
                                                            String group,
                                                            String topics,
                                                            String offset,
                                                            String componentId,
                                                            int parallelism) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", kafkaServers);
        properties.put("zookeeper.connect", zkConStr);
        properties.put("group.id", group);
        properties.put("topic", topics);
        properties.put("auto.commit.enable", false);
        properties.put("auto.offset.reset", offset);

        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer082<String>(topics, new SimpleStringSchema(), properties));
        return new FlinkWorkloadOperator<>(stream);
    }

    @Override
    public void Start() {
        try {
            env.execute(this.getAppName());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
