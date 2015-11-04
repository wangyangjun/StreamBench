package fi.aalto.dmg.frame;

import fi.aalto.dmg.frame.functions.*;
import fi.aalto.dmg.util.TimeDurations;
import scala.Tuple2;

import java.io.Serializable;

/**
 * Created by yangjun.wang on 24/10/15.
 */
public interface PairWorkloadOperator<K, V> extends WorkloadOperator<Tuple2<K,V>> , Serializable{

    GroupedWorkloadOperator<K,V> groupByKey();

    // TODO: translate to reduce on each node, then group merge
    PairWorkloadOperator<K, V> reduceByKey(ReduceFunction<V> fun, String componentId);

    PairWorkloadOperator<K, V> updateStateByKey(UpdateStateFunction<V> fun, String componentId);

    PairWorkloadOperator<K, V> reduceByKeyAndWindow(ReduceFunction<V> fun, TimeDurations windowDuration, TimeDurations slideDuration);

    @Override
    WindowedPairWorkloadOperator<K, V> window(TimeDurations windowDuration);
    @Override
    WindowedPairWorkloadOperator<K, V> window(TimeDurations windowDuration, TimeDurations slideDuration);

    // TODO: new APIs filter, mapValue
}

