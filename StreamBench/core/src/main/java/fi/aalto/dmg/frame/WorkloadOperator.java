package fi.aalto.dmg.frame;

import fi.aalto.dmg.frame.functions.*;

import java.io.Serializable;

/**
 * Created by yangjun.wang on 21/10/15.
 */
public interface WorkloadOperator<T> extends Serializable {

    // return new WorkloadOperator<R>();
    <R> WorkloadOperator<R> map(MapFunction<T, R> fun);

    // return WorkloadOperator<R>
    <R> WorkloadOperator<R> mapPartition(MapPartitionFunction<T, R> fun);

    // return new WorkloadPairOperator<K,V>
    <K, V> WorkloadPairOperator<K, V> mapToPair(MapPairFunction<T, K, V> fun);

    // return new WorkloadOperator<T>();
    WorkloadOperator<T> reduce(ReduceFunction<T> fun);

    // return new WorkloadOperator<T>();
    WorkloadOperator<T> filter(FilterFunction<T> fun);

    // return new WorkloadOperator<R>();
    <R> WorkloadOperator<R> flatMap(FlatMapFunction<T, R> fun);

    void print();
}
