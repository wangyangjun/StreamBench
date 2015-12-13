package fi.aalto.dmg.frame;

import fi.aalto.dmg.frame.functions.*;
import fi.aalto.dmg.util.TimeDurations;
import java.io.Serializable;

/**
 * Created by yangjun.wang on 21/10/15.
 */
public interface WorkloadOperator<T> extends Serializable {

    /** Map T to R for each entity */
    <R> WorkloadOperator<R> map(MapFunction<T, R> fun, String componentId, boolean logThroughput);
    <R> WorkloadOperator<R> map(MapFunction<T, R> fun, String componentId);

    /** Map T to Pair<K,V>, return PairWorkloadOperator */
    <K, V> PairWorkloadOperator<K, V> mapToPair(MapPairFunction<T, K, V> fun, String componentId, boolean logThroughput);
    <K, V> PairWorkloadOperator<K, V> mapToPair(MapPairFunction<T, K, V> fun, String componentId);

    /** reduce on whole stream */
    WorkloadOperator<T> reduce(ReduceFunction<T> fun, String componentId, boolean logThroughput);
    WorkloadOperator<T> reduce(ReduceFunction<T> fun, String componentId);

    /** filter entity if fun(entity) is false */
    WorkloadOperator<T> filter(FilterFunction<T> fun, String componentId, boolean logThroughput);
    WorkloadOperator<T> filter(FilterFunction<T> fun, String componentId);

    /**
     * iterative operator,
     * @param mapFunction
     * @param iterativeFunction
     *      if return yes, then iterator
     * @param componentId
     * @return
     */
    WorkloadOperator<T> iterative(MapFunction<T, T> mapFunction, FilterFunction<T> iterativeFunction, String componentId);

    /** Map T to iterable<R> */
    <R> WorkloadOperator<R> flatMap(FlatMapFunction<T, R> fun, String componentId, boolean logThroughput);
    <R> WorkloadOperator<R> flatMap(FlatMapFunction<T, R> fun, String componentId);


    WindowedWorkloadOperator<T> window(TimeDurations windowDuration);
    WindowedWorkloadOperator<T> window(TimeDurations windowDuration, TimeDurations slideDuration);

    void print();

    void sink();
}
