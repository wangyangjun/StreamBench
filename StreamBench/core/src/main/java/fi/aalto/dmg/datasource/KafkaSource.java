package fi.aalto.dmg.datasource;

import fi.aalto.dmg.BenchStarter;
import fi.aalto.dmg.exceptions.WorkloadException;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by yangjun.wang on 14/10/15.
 */
public class KafkaSource {

    private final Logger logger;
    private final static String DEFAULT_ZOOKEEPER_CONNECT = "localhost:2181";
    private final static String DEFAULT_KAFKA_CONSUMER_OFFSET = "smallest";
    private final static String DEFAULT_KAFKA_PRODUCER_SERVER = "localhost:9092";


    private Properties properties;

    public KafkaSource() throws WorkloadException {
        logger = Logger.getLogger(this.getClass());
        properties = new Properties();
        try {
            properties.load(this.getClass().getClassLoader().getResourceAsStream(BenchStarter.CONFIGURE));
        } catch (IOException e) {
            throw new WorkloadException("Read configure file " + BenchStarter.CONFIGURE + " failed");
        }
    }

    public String getKafkaZookeeperConnect(){
        String zookeeper = properties.getProperty("systems.kafka.consumer.zookeeper.connect");
        if(null == zookeeper)
            return DEFAULT_ZOOKEEPER_CONNECT;
        return zookeeper;
    }

    public String getKafkaProducerServers(){
        String producerServers = properties.getProperty("systems.kafka.producer.bootstrap.servers");
        if( null == producerServers )
            return DEFAULT_KAFKA_PRODUCER_SERVER;
        return producerServers;
    }

    public String getKafkaConsumerOffset(){
        String consumerOffset = properties.getProperty("systems.kafka.consumer.auto.offset.reset");
        if( null == consumerOffset )
            return DEFAULT_KAFKA_CONSUMER_OFFSET;
        return consumerOffset;
    }

}
