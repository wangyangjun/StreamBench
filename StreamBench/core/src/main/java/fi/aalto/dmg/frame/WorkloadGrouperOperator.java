package fi.aalto.dmg.frame;

import fi.aalto.dmg.frame.functions.ReduceFunction;

import java.io.Serializable;

/**
 * Created by yangjun.wang on 25/10/15.
 */
public interface WorkloadGrouperOperator<K,V> extends Serializable{
    WorkloadPairOperator<K, V> reduce(ReduceFunction<V> fun);
}
