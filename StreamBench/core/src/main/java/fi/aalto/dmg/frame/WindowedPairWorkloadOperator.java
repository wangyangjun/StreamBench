package fi.aalto.dmg.frame;

import fi.aalto.dmg.frame.functions.*;
import scala.Tuple2;

import java.io.Serializable;

/**
 * Created by jun on 11/3/15.
 */
public interface WindowedPairWorkloadOperator<K,V> extends Serializable {

    /**
     *
     * @param fun
     * @param componentId
     * @return PairWorkloadOperator<K, V>
     */
    PairWorkloadOperator<K, V> reduceByKey(ReduceFunction<V> fun, String componentId);

    /**
     * cumulate window stream
     * @param fun
     * @param componentId
     * @return
     */
    PairWorkloadOperator<K, V> updateStateByKey(ReduceFunction<V> fun, String componentId);

    // return WorkloadOperator<R>
    <R> PairWorkloadOperator<K, R> mapPartition(MapPartitionFunction<Tuple2<K,V>, Tuple2<K,R>> fun, String componentId);

    // return new WorkloadOperator<R>();
    <R> PairWorkloadOperator<K, R> mapValue(MapFunction<Tuple2<K,V>, Tuple2<K,R>> fun, String componentId);

    // return new WorkloadOperator<T>();
    PairWorkloadOperator<K, V> filter(FilterFunction<Tuple2<K,V>> fun, String componentId);

    PairWorkloadOperator<K, V> reduce(ReduceFunction<Tuple2<K,V>> fun, String componentId);

    void print();

}
