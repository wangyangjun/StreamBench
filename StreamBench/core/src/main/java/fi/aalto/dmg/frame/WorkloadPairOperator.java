package fi.aalto.dmg.frame;

import fi.aalto.dmg.frame.functions.ReduceFunction;
import fi.aalto.dmg.frame.functions.UpdateStateFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Iterator;

/**
 * Created by yangjun.wang on 24/10/15.
 */
public interface WorkloadPairOperator<K, V> extends WorkloadOperator<Tuple2<K,V>> , Serializable{

    WorkloadGrouperOperator<K,V> groupByKey();

    // TODO: translate to reduce on each node, then group merge
    WorkloadPairOperator<K, V> reduceByKey(K key, ReduceFunction<V> fun);

    WorkloadPairOperator<K, V> updateStateByKey(UpdateStateFunction<V> fun);

}

