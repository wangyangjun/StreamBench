package fi.aalto.dmg.workloads;

import com.google.common.base.Optional;
import fi.aalto.dmg.Workload;
import fi.aalto.dmg.exceptions.WorkloadException;
import fi.aalto.dmg.frame.WorkloadOperator;
import fi.aalto.dmg.frame.OperatorCreater;
import fi.aalto.dmg.frame.WorkloadPairOperator;
import fi.aalto.dmg.frame.functions.FlatMapFunction;
import fi.aalto.dmg.frame.functions.MapPairFunction;
import fi.aalto.dmg.frame.functions.ReduceFunction;
import fi.aalto.dmg.frame.functions.UpdateStateFunction;
import fi.aalto.dmg.frame.userfunctions.UserFunctions;
import org.apache.log4j.Logger;
import scala.Tuple2;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;

/**
 * Created by yangjun.wang on 27/10/15.
 */
public class WordCount extends Workload implements Serializable{

    private static final Logger logger = Logger.getLogger(WordCount.class);

    public WordCount(OperatorCreater creater) throws WorkloadException {
        super(creater);
    }

    public void Process() throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        // ClassLoader classLoader = WordCount.class.getClassLoader();
        // Class dbclass = classLoader.loadClass("fi.aalto.dmg.frame.SparkOperatorCreater");
        // OperatorCreater operatorCreater = (OperatorCreater)dbclass.getConstructor(String.class).newInstance("WordCount");
        try {
            WorkloadOperator<String> operator =
                    this.getOperatorCreater().createOperatorFromKafka("localhost:2181", "asdf", "WordCount").map(UserFunctions.mapToSelf);
            WorkloadPairOperator<String, Integer> counts =
                    operator.flatMap(UserFunctions.splitFlatMap).
                            mapToPair(UserFunctions.mapToStringIntegerPair).
                            groupByKey().reduce(UserFunctions.sumReduce).
                            updateStateByKey(UserFunctions.updateStateCount);
            counts.print();
        }
        catch (Exception e){
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }
}
