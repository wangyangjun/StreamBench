package fi.aalto.dmg.workloads;

import fi.aalto.dmg.exceptions.WorkloadException;
import fi.aalto.dmg.frame.OperatorCreator;
import fi.aalto.dmg.frame.WorkloadOperator;
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
abstract public class Workload implements Serializable{
    private static final Logger logger = LoggerFactory.getLogger(Workload.class);

    private Properties properties;
    private OperatorCreator operatorCreator;
    protected int parallelism;

    public Workload(OperatorCreator creater) throws WorkloadException {
        this.operatorCreator = creater;
        properties = new Properties();
        String configFile = this.getClass().getSimpleName() + ".properties";
        try {
            properties.load(this.getClass().getClassLoader().getResourceAsStream(configFile));

            int hosts = Integer.parseInt(properties.getProperty("hosts"));
            int cores = Integer.parseInt(properties.getProperty("cores"));
            parallelism = hosts*cores;

        } catch (IOException e) {
            throw new WorkloadException("Read configure file " + configFile + " failed");
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Read configure file: " + configFile + " failed");
        }

    }

    protected Properties getProperties() {
        return properties;
    }
    protected OperatorCreator getOperatorCreator(){ return operatorCreator; }

    protected WorkloadOperator<WithTime<String>> kafkaStreamOperatorWithTime(){
        String topic = this.getProperties().getProperty("topic");
        String groupId = this.getProperties().getProperty("group.id");
        String kafkaServers = this.getProperties().getProperty("bootstrap.servers");
        String zkConnectStr = this.getProperties().getProperty("zookeeper.connect");
        String offset = this.getProperties().getProperty("auto.offset.reset");

        return this.getOperatorCreator().createOperatorFromKafkaWithTime(zkConnectStr,
                kafkaServers, groupId, topic, offset, parallelism);
    }

    protected WorkloadOperator<String> kafkaStreamOperator(){
        String topic = this.getProperties().getProperty("topic");
        String groupId = this.getProperties().getProperty("group.id");
        String kafkaServers = this.getProperties().getProperty("bootstrap.servers");
        String zkConnectStr = this.getProperties().getProperty("zookeeper.connect");
        String offset = this.getProperties().getProperty("auto.offset.reset");

        return this.getOperatorCreator().createOperatorFromKafka(zkConnectStr,
                kafkaServers, groupId, topic, offset, parallelism);
    }

    public void Start(){
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
