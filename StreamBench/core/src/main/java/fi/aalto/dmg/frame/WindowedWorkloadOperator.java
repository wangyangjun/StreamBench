package fi.aalto.dmg.frame;

import fi.aalto.dmg.frame.functions.*;

import java.io.Serializable;

/**
 * Created by yangjun.wang on 31/10/15.
 */
abstract public class WindowedWorkloadOperator<T> extends OperatorBase {

    public WindowedWorkloadOperator(int parallelism) {
        super(parallelism);
    }

    // return WorkloadOperator<R>
    abstract public <R> WorkloadOperator<R> mapPartition(MapPartitionFunction<T, R> fun, String componentId);

    // return new WorkloadOperator<R>();
    abstract public <R> WorkloadOperator<R> map(MapFunction<T, R> fun, String componentId);

    // return new WorkloadOperator<T>();
    abstract public WorkloadOperator<T> filter(FilterFunction<T> fun, String componentId);

    // return new WorkloadOperator<T>();
    abstract public WorkloadOperator<T> reduce(ReduceFunction<T> fun, String componentId);

    // return new PairWorkloadOperator<K,V>
    abstract public <K, V> PairWorkloadOperator<K, V> mapToPair(MapPairFunction<T, K, V> fun, String componentId);

}
