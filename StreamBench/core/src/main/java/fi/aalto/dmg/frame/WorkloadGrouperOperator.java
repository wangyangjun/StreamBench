package fi.aalto.dmg.frame;

import fi.aalto.dmg.frame.functions.ReduceFunction;

/**
 * Created by yangjun.wang on 25/10/15.
 */
public interface WorkloadGrouperOperator<K,V> {
    WorkloadPairOperator<K, V> reduce(ReduceFunction<V> fun);
}
