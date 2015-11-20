package fi.aalto.dmg.workloads;

import fi.aalto.dmg.Workload;
import fi.aalto.dmg.exceptions.WorkloadException;
import fi.aalto.dmg.frame.PairWorkloadOperator;
import fi.aalto.dmg.frame.WorkloadOperator;
import fi.aalto.dmg.frame.OperatorCreater;
import fi.aalto.dmg.frame.userfunctions.UserFunctions;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;

/**
 * Word count workload
 * Spark, Flink implement pre-aggregation
 * Storm reduces on field grouping stream
 * Spark streaming is a mini-batches model, which need call updateStateByKey to accumulate
 * Created by yangjun.wang on 27/10/15.
 */
public class WordCount extends Workload implements Serializable{

    private static final Logger logger = Logger.getLogger(WordCount.class);
    private static final long serialVersionUID = -1558126580235739604L;

    public WordCount(OperatorCreater creater) throws WorkloadException {
        super(creater);
    }

    private WorkloadOperator<String> kafkaStreamOperator(){
        String topic = this.getProperties().getProperty("topic");
        String groupId = this.getProperties().getProperty("group.id");
        String kafkaServers = this.getProperties().getProperty("bootstrap.servers");
        String zkConnectStr = this.getProperties().getProperty("zookeeper.connect");
        String offset = this.getProperties().getProperty("auto.offset.reset");

        return this.getOperatorCreater().createOperatorFromKafka(zkConnectStr, kafkaServers, groupId, topic, offset);
    }

    public void Process() throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        try {
            WorkloadOperator<String> operator = kafkaStreamOperator();
            PairWorkloadOperator<String, Integer> counts =
                    operator.flatMap(UserFunctions.splitFlatMap, "spliter")
                            .mapToPair(UserFunctions.mapToStringIntegerPair, "pair")
                            .reduceByKey(UserFunctions.sumReduce, "sum")
                            .updateStateByKey(UserFunctions.sumReduce, "cumulate");
            counts.print();
        }
        catch (Exception e){
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }
}
