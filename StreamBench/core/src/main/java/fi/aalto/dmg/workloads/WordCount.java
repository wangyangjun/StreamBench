package fi.aalto.dmg.workloads;

import fi.aalto.dmg.exceptions.WorkloadException;
import fi.aalto.dmg.frame.PairWorkloadOperator;
import fi.aalto.dmg.frame.WorkloadOperator;
import fi.aalto.dmg.frame.OperatorCreator;
import fi.aalto.dmg.frame.userfunctions.UserFunctions;
import fi.aalto.dmg.util.WithTime;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;

/**
 * Word count workload
 * Spark, Flink implement pre-aggregation
 * Storm reduces on field grouping stream
 * Spark streaming is a mini-batches model, which need call updateStateByKey to accumulate
 * <p>
 * *********** Components ***************
 * Source -> AddTime -> Splitter -> Pair -> Sum -> (Update state) -> Sink(log latency)
 * Created by yangjun.wang on 27/10/15.
 */
public class WordCount extends Workload implements Serializable {

    private static final Logger logger = Logger.getLogger(WordCount.class);
    private static final long serialVersionUID = -1558126580235739604L;

    public WordCount(OperatorCreator creator) throws WorkloadException {
        super(creator);
    }


    public void Process() throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        try {

            WorkloadOperator<WithTime<String>> operator = stringStreamWithTime("source");
            PairWorkloadOperator<String, WithTime<Integer>> counts =
                    operator.flatMap(UserFunctions.splitFlatMapWithTime, "splitter")
                            .mapToPair(UserFunctions.mapToStrIntPairWithTime, "pair")
                            .reduceByKey(UserFunctions.sumReduceWithTime, "sum")
                            .updateStateByKey(UserFunctions.sumReduceWithTime, "accumulate");
            counts.sink();
//            counts.print();
        } catch (Exception e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }
}
