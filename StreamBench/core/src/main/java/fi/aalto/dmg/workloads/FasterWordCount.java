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
 * Created by jun on 19/12/15.
 */
public class FasterWordCount extends Workload implements Serializable {
    private static final long serialVersionUID = -3798405308281537073L;
    private static final Logger logger = Logger.getLogger(WordCount.class);

    public FasterWordCount(OperatorCreator creater) throws WorkloadException {
        super(creater);
    }

    @Override
    public void Process() throws WorkloadException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        try {

            WorkloadOperator<String> operator = kafkaStreamOperator("source");
            PairWorkloadOperator<String, WithTime<Integer>> counts =
                    operator.flatMapToPair(UserFunctions.flatMapToPairAddTime, "splitter")
                            .reduceByKey(UserFunctions.sumReduceWithTime, "sum")
                            .updateStateByKey(UserFunctions.sumReduceWithTime, "accumulate");
            counts.sink();
        } catch (Exception e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }
}
