package fi.aalto.dmg.workloads;

import fi.aalto.dmg.Workload;
import fi.aalto.dmg.exceptions.WorkloadException;
import fi.aalto.dmg.frame.OperatorCreater;
import fi.aalto.dmg.frame.WorkloadOperator;
import fi.aalto.dmg.frame.functions.ReduceFunction;
import fi.aalto.dmg.frame.userfunctions.UserFunctions;
import fi.aalto.dmg.util.TimeDurations;
import org.apache.log4j.Logger;
import scala.Tuple2;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.TimeUnit;

/**
 * Created by yangjun.wang on 03/11/15.
 */
public class WordCountWindowed  extends Workload implements Serializable {
    private static final Logger logger = Logger.getLogger(WordCountWindowed.class);

    public WordCountWindowed(OperatorCreater creater) throws WorkloadException {
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

    public static ReduceFunction<Tuple2<String, Integer>> tuple2ReduceFunction = new ReduceFunction<Tuple2<String, Integer>>() {
        @Override
        public Tuple2<String, Integer> reduce(Tuple2<String, Integer> var1, Tuple2<String, Integer> var2) throws Exception {
            return new Tuple2<String, Integer>(var1._1(), var1._2()+var2._2());
        }
    };
    @Override
    public void Process() throws WorkloadException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        try {
            WorkloadOperator<String> operator = kafkaStreamOperator();
            WorkloadOperator<Tuple2<String, Integer>> counts =
                    operator.flatMap(UserFunctions.splitFlatMap, "spliter")
                            .mapToPair(UserFunctions.mapToStringIntegerPair, "pair")
                            .groupByKey()
                            .window(new TimeDurations(TimeUnit.SECONDS, 5))
                            .reduce(tuple2ReduceFunction, "sum");
            counts.print();
        }
        catch (Exception e){
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }
}
