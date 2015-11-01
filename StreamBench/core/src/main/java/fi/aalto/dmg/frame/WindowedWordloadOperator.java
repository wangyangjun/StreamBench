package fi.aalto.dmg.frame;

import fi.aalto.dmg.frame.functions.MapPartitionFunction;

import java.io.Serializable;

/**
 * Created by yangjun.wang on 31/10/15.
 */
public interface WindowedWordloadOperator<T> extends Serializable {

    // return WorkloadOperator<R>
    <R> WorkloadOperator<R> mapPartition(MapPartitionFunction<T, R> fun, String componentId);

}
