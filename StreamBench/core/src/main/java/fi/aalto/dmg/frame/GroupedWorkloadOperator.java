package fi.aalto.dmg.frame;

import fi.aalto.dmg.frame.functions.ReduceFunction;
import fi.aalto.dmg.util.TimeDurations;
import scala.Tuple2;

import java.io.Serializable;

/**
 * Created by yangjun.wang on 25/10/15.
 */
abstract public class GroupedWorkloadOperator<K, V> extends OperatorBase implements Serializable {
    public GroupedWorkloadOperator(int parallelism) {
        super(parallelism);
    }

    abstract public PairWorkloadOperator<K, V> reduce(ReduceFunction<V> fun, String componentId, int parallelism);
}
