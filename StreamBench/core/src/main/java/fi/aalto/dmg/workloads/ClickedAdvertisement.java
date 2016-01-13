package fi.aalto.dmg.workloads;

import fi.aalto.dmg.exceptions.WorkloadException;
import fi.aalto.dmg.frame.OperatorCreator;
import fi.aalto.dmg.frame.PairWorkloadOperator;
import fi.aalto.dmg.frame.WorkloadOperator;
import fi.aalto.dmg.frame.functions.AssignTimeFunction;
import fi.aalto.dmg.frame.functions.MapPairFunction;
import fi.aalto.dmg.frame.userfunctions.UserFunctions;
import fi.aalto.dmg.util.TimeDurations;
import fi.aalto.dmg.util.WithTime;
import org.apache.log4j.Logger;
import scala.Tuple2;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.TimeUnit;

/**
 * Created by jun on 09/01/16.
 */
public class ClickedAdvertisement extends Workload implements Serializable {
    private static final long serialVersionUID = -7308411680185973067L;

    private static final Logger logger = Logger.getLogger(WordCount.class);

    public ClickedAdvertisement(OperatorCreator creater) throws WorkloadException {
        super(creater);
    }

    private static class TimeAssigner implements AssignTimeFunction<Long>, Serializable{
        @Override
        public long assign(Long var1) {
            return var1;
        }
    }
    @Override
    public void Process() throws WorkloadException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        try {

            PairWorkloadOperator<String, Long> advertisements = kafkaStreamOperator("advertisement")
                    .mapToPair(UserFunctions.mapToStringLongPair, "Extractor", parallelism);
            PairWorkloadOperator<String, Long> clicks = kafkaStreamOperator2("click")
                    .mapToPair(UserFunctions.mapToStringLongPair, "Extractor2", parallelism );
//            clicks.print();
            PairWorkloadOperator<String, Tuple2<Long, Long>> clicksWithCreatTime = advertisements.join(
                    "Join",
                    clicks,
                    new TimeDurations(TimeUnit.MINUTES, 1),
                    new TimeDurations(TimeUnit.MINUTES, 1),
                    new TimeAssigner(),
                    new TimeAssigner());
            clicksWithCreatTime.print();
        }
        catch (Exception e){
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }
}
