package fi.aalto.dmg.frame;

import fi.aalto.dmg.frame.functions.*;

/**
 * Created by yangjun.wang on 21/10/15.
 */
public interface WorkloadOperator<T> {

    // return new WorkloadOperator<R>();
    <R> WorkloadOperator<R> map(MapFunction<T, R> fun);

    // return new WorkloadPairOperator<K,V>
    <K, V> WorkloadPairOperator<K, V> mapToPair(MapPairFunction<T, K, V> fun);

    // return new WorkloadOperator<T>();
    WorkloadOperator<T> reduce(ReduceFunction<T> fun);

    // return new WorkloadOperator<T>();
    WorkloadOperator<T> filter(FilterFunction<T> fun);

    // return new WorkloadOperator<R>();
    <R> WorkloadOperator<R> flatMap(FlatMapFunction<T, R> fun);

}
