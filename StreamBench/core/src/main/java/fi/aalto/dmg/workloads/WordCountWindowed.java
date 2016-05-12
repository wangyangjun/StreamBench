package fi.aalto.dmg.workloads;

import fi.aalto.dmg.exceptions.WorkloadException;
import fi.aalto.dmg.frame.OperatorCreator;
import fi.aalto.dmg.frame.PairWorkloadOperator;
import fi.aalto.dmg.frame.WorkloadOperator;
import fi.aalto.dmg.frame.functions.ReduceFunction;
import fi.aalto.dmg.frame.userfunctions.UserFunctions;
import fi.aalto.dmg.util.TimeDurations;
import fi.aalto.dmg.util.WithTime;
import org.apache.log4j.Logger;
import scala.Tuple2;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.TimeUnit;

/**
 * Created by yangjun.wang on 03/11/15.
 */
public class WordCountWindowed extends Workload implements Serializable {
    private static final Logger logger = Logger.getLogger(WordCountWindowed.class);
    private static final long serialVersionUID = 5131563712627441022L;

    public WordCountWindowed(OperatorCreator creater) throws WorkloadException {
        super(creater);
    }

    @Override
    public void Process() throws WorkloadException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        try {
            // Flink doesn't support shuffle().window()
            // Actually Flink does keyGrouping().window().update()
            // It is the same situation to Spark streaming
            WorkloadOperator<WithTime<String>> operator = stringStreamWithTime("source");
            PairWorkloadOperator<String, WithTime<Integer>> counts =
                    operator.flatMap(UserFunctions.splitFlatMapWithTime, "splitter")
                            .mapToPair(UserFunctions.mapToStrIntPairWithTime, "pair")
                            .reduceByKeyAndWindow(UserFunctions.sumReduceWithTime2, "counter",
                                    new TimeDurations(TimeUnit.SECONDS, 1), new TimeDurations(TimeUnit.SECONDS, 1));
            counts.sink();

            // cumulate counts
//            PairWorkloadOperator<String, Integer> cumulateCounts =counts.updateStateByKey(UserFunctions.sumReduce, "cumulate");
//            cumulateCounts.print();

        } catch (Exception e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }
}
