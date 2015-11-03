package fi.aalto.dmg.frame;

import fi.aalto.dmg.frame.functions.*;
import fi.aalto.dmg.util.TimeDurations;
import java.io.Serializable;

/**
 * Created by yangjun.wang on 21/10/15.
 */
public interface WorkloadOperator<T> extends Serializable {

    // return new WorkloadOperator<R>();
    <R> WorkloadOperator<R> map(MapFunction<T, R> fun, String componentId);

    // return new PairedWorkloadOperator<K,V>
    <K, V> PairedWorkloadOperator<K, V> mapToPair(MapPairFunction<T, K, V> fun, String componentId);

    // return new WorkloadOperator<T>();
    WorkloadOperator<T> reduce(ReduceFunction<T> fun, String componentId);

    // return new WorkloadOperator<T>();
    WorkloadOperator<T> filter(FilterFunction<T> fun, String componentId);

    // return new WorkloadOperator<R>();
    <R> WorkloadOperator<R> flatMap(FlatMapFunction<T, R> fun, String componentId);

    WindowedWordloadOperator<T> window(TimeDurations windowDuration);
    WindowedWordloadOperator<T> window(TimeDurations windowDuration, TimeDurations slideDuration);

    void print();
}
