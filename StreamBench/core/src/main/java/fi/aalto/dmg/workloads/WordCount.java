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
                    operator.flatMap(new FlatMapFunction<String, String>() {
                        public Iterable<String> flatMap(String var1) throws Exception {
                            return Arrays.asList(var1.toLowerCase().split("\\W+"));
                        }
                    }).
                            mapToPair(new MapPairFunction<String, String, Integer>() {
                                public Tuple2<String, Integer> mapPair(String s) {
                                    return new Tuple2<String, Integer>(s, 1);
                                }
                            }).
                            groupByKey().reduce(new ReduceFunction<Integer>() {
                        public Integer reduce(Integer var1, Integer var2) throws Exception {
                            return var1 + var2;
                        }
                    }).
                            updateStateByKey(new UpdateStateFunction<Integer>() {
                                public Optional<Integer> update(List<Integer> values, Optional<Integer> cumulateValue) {
                                    Integer sum = cumulateValue.or(0);
                                    for (Integer i : values) {
                                        sum += i;
                                    }
                                    return Optional.of(sum);
                                }
                            });
            counts.print();
        }
        catch (Exception e){
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }
}
