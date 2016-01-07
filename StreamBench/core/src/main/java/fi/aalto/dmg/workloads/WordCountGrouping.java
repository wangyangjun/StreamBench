package fi.aalto.dmg.workloads;

import fi.aalto.dmg.exceptions.WorkloadException;
import fi.aalto.dmg.frame.OperatorCreator;
import fi.aalto.dmg.frame.PairWorkloadOperator;
import fi.aalto.dmg.frame.WorkloadOperator;
import fi.aalto.dmg.frame.userfunctions.UserFunctions;
import fi.aalto.dmg.util.WithTime;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;

/**
 * Created by yangjun.wang on 30/10/15.
 * There is no difference between WordCount and WordCountGrouping in Flink
 *
 */
public class WordCountGrouping extends Workload implements Serializable {

    private static final Logger logger = Logger.getLogger(WordCount.class);
    private static final long serialVersionUID = 2835367895719829656L;

    public WordCountGrouping(OperatorCreator creater) throws WorkloadException {
        super(creater);
    }

    public void Process() throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        try {
            int hosts = Integer.parseInt(this.getProperties().getProperty("hosts"));
            int cores = Integer.parseInt(this.getProperties().getProperty("cores"));
            int parallelism = hosts*cores;

            WorkloadOperator<WithTime<String>> operator = kafkaStreamOperatorWithTime();
            PairWorkloadOperator<String, WithTime<Integer>> counts =
                    operator.flatMap(UserFunctions.splitFlatMapWithTime, "spliter", parallelism).
                            mapToPair(UserFunctions.mapToStrIntPairWithTime, "pair", parallelism).
                            groupByKey().reduce(UserFunctions.sumReduceWithTime, "sum", parallelism).
                            updateStateByKey(UserFunctions.sumReduceWithTime, "cumulate", parallelism);
            counts.print();
        }
        catch (Exception e){
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }
}
