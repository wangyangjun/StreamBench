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
public class WordCountWindowed  extends Workload implements Serializable {
    private static final Logger logger = Logger.getLogger(WordCountWindowed.class);
    private static final long serialVersionUID = 5131563712627441022L;

    public WordCountWindowed(OperatorCreator creater) throws WorkloadException {
        super(creater);
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
            int hosts = Integer.parseInt(properties.getProperty("hosts"));
            int cores = Integer.parseInt(properties.getProperty("cores"));
            int parallelism = hosts*cores;


            // Flink doesn't support shuffle().window()
            // Actually Flink does keyGrouping().window().update()
            // It is the same situation to Spark streaming
            WorkloadOperator<WithTime<String>> operator = stringStreamWithTime("source");
            PairWorkloadOperator<String, WithTime<Integer>> counts =
                    operator.flatMap(UserFunctions.splitFlatMapWithTime, "splitter")
                            .mapToPair(UserFunctions.mapToStrIntPairWithTime, "pair")
                            .reduceByKeyAndWindow(UserFunctions.sumReduceWithTime, "counter",
                                    new TimeDurations(TimeUnit.SECONDS, 1), new TimeDurations(TimeUnit.SECONDS, 1));
            counts.print();

            // cumulate counts
//            PairWorkloadOperator<String, Integer> cumulateCounts =counts.updateStateByKey(UserFunctions.sumReduce, "cumulate");
//            cumulateCounts.print();

        }
        catch (Exception e){
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }
}
