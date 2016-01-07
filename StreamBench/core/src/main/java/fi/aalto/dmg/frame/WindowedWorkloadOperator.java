package fi.aalto.dmg.frame;

import fi.aalto.dmg.frame.functions.*;

import java.io.Serializable;

/**
 * Created by yangjun.wang on 31/10/15.
 */
public interface WindowedWorkloadOperator<T> extends Serializable {

    // return WorkloadOperator<R>
    <R> WorkloadOperator<R> mapPartition(MapPartitionFunction<T, R> fun, String componentId, int parallelism, boolean logThroughput);
    <R> WorkloadOperator<R> mapPartition(MapPartitionFunction<T, R> fun, String componentId, int parallelism);

    // return new WorkloadOperator<R>();
    <R> WorkloadOperator<R> map(MapFunction<T, R> fun, String componentId, int parallelism, boolean logThroughput);
    <R> WorkloadOperator<R> map(MapFunction<T, R> fun, String componentId, int parallelism);

    // return new WorkloadOperator<T>();
    WorkloadOperator<T> filter(FilterFunction<T> fun, String componentId, int parallelism, boolean logThroughput);
    WorkloadOperator<T> filter(FilterFunction<T> fun, String componentId, int parallelism);

    // return new WorkloadOperator<T>();
    WorkloadOperator<T> reduce(ReduceFunction<T> fun, String componentId, int parallelism, boolean logThroughput);
    WorkloadOperator<T> reduce(ReduceFunction<T> fun, String componentId, int parallelism);

    // return new PairWorkloadOperator<K,V>
    <K, V> PairWorkloadOperator<K, V> mapToPair(MapPairFunction<T, K, V> fun, String componentId, int parallelism, boolean logThroughput);
    <K, V> PairWorkloadOperator<K, V> mapToPair(MapPairFunction<T, K, V> fun, String componentId, int parallelism);

    void print();
}
