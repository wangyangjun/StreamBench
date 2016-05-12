package fi.aalto.dmg.workloads;

import fi.aalto.dmg.exceptions.WorkloadException;
import fi.aalto.dmg.frame.OperatorCreator;
import fi.aalto.dmg.frame.WorkloadOperator;
import fi.aalto.dmg.util.Configure;
import fi.aalto.dmg.util.Point;
import fi.aalto.dmg.util.WithTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

/**
 * Created by yangjun.wang on 14/10/15.
 */
abstract public class Workload implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(Workload.class);

    protected Properties properties;
    private OperatorCreator operatorCreator;
    protected int parallelism;

    public Workload(OperatorCreator creator) throws WorkloadException {
        this.operatorCreator = creator;
        Configure.LoadConfigure();
        parallelism = Configure.clusterHosts * Configure.hostCores;

        // load specific configure for each workload
        properties = new Properties();
        String configFile = this.getClass().getSimpleName() + ".properties";
        try {
            properties.load(this.getClass().getClassLoader().getResourceAsStream(configFile));

//            int hosts = Integer.parseInt(properties.getProperty("hosts"));
//            int cores = Integer.parseInt(properties.getProperty("cores"));
        } catch (IOException e) {
            throw new WorkloadException("Read configure file " + configFile + " failed");
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Read configure file: " + configFile + " failed");
        }

    }

    protected OperatorCreator getOperatorCreator() {
        return operatorCreator;
    }

    protected WorkloadOperator<WithTime<String>> stringStreamWithTime(String componentId) {
        String topic = properties.getProperty("topic");
        String groupId = properties.getProperty("group.id");
        String kafkaServers = properties.getProperty("bootstrap.servers");
        String zkConnectStr = properties.getProperty("zookeeper.connect");
        String offset = properties.getProperty("auto.offset.reset");

        return this.getOperatorCreator().stringStreamFromKafkaWithTime(zkConnectStr,
                kafkaServers, groupId, topic, offset, componentId, parallelism);
    }

    protected WorkloadOperator<Point> getPointStream(String componentId) {
        String topic = properties.getProperty("topic");
        String groupId = properties.getProperty("group.id");
        String kafkaServers = properties.getProperty("bootstrap.servers");
        String zkConnectStr = properties.getProperty("zookeeper.connect");
        String offset = properties.getProperty("auto.offset.reset");

        return this.getOperatorCreator().pointStreamFromKafka(zkConnectStr,
                kafkaServers, groupId, topic, offset, componentId, parallelism);
    }

    protected WorkloadOperator<String> kafkaStreamOperator(String componentId) {
        String topic = properties.getProperty("topic");
        String groupId = properties.getProperty("group.id");
        String kafkaServers = properties.getProperty("bootstrap.servers");
        String zkConnectStr = properties.getProperty("zookeeper.connect");
        String offset = properties.getProperty("auto.offset.reset");

        return this.getOperatorCreator().stringStreamFromKafka(zkConnectStr,
                kafkaServers, groupId, topic, offset, componentId, parallelism);
    }

    protected WorkloadOperator<String> kafkaStreamOperator2(String componentId) {
        String topic = properties.getProperty("topic2");
        String groupId = properties.getProperty("group.id");
        String kafkaServers = properties.getProperty("bootstrap.servers");
        String zkConnectStr = properties.getProperty("zookeeper.connect");
        String offset = properties.getProperty("auto.offset.reset");

        return this.getOperatorCreator().stringStreamFromKafka(zkConnectStr,
                kafkaServers, groupId, topic, offset, componentId, parallelism);
    }

    public void Start() {
        logger.info("Start workload: " + this.getClass().getSimpleName());
        try {
            Process();
            this.getOperatorCreator().Start();
        } catch (Exception e) {
            logger.error("WorkloadException caught when run workload " + this.getClass().getSimpleName());
            e.printStackTrace();
        }
        logger.info("The end of workload: " + this.getClass().getSimpleName());
    }

    abstract public void Process() throws WorkloadException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException;

}
