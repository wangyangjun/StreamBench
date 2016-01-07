package fi.aalto.dmg.frame;

import fi.aalto.dmg.frame.functions.ReduceFunction;
import fi.aalto.dmg.util.TimeDurations;
import scala.Tuple2;

import java.io.Serializable;

/**
 * Created by yangjun.wang on 25/10/15.
 */
public interface GroupedWorkloadOperator<K,V> extends Serializable{
    PairWorkloadOperator<K, V> reduce(ReduceFunction<V> fun, String componentId, int parallelism);
}
