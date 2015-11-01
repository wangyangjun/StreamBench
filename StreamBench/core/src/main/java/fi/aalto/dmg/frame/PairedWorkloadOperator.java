package fi.aalto.dmg.frame;

import fi.aalto.dmg.frame.functions.ReduceFunction;
import fi.aalto.dmg.frame.functions.UpdateStateFunction;
import scala.Tuple2;

import java.io.Serializable;

/**
 * Created by yangjun.wang on 24/10/15.
 */
public interface PairedWorkloadOperator<K, V> extends WorkloadOperator<Tuple2<K,V>> , Serializable{

    GroupedWorkloadOperator<K,V> groupByKey();

    // TODO: translate to reduce on each node, then group merge
    PairedWorkloadOperator<K, V> reduceByKey(ReduceFunction<V> fun, String componentId);

    PairedWorkloadOperator<K, V> updateStateByKey(UpdateStateFunction<V> fun, String componentId);

}

