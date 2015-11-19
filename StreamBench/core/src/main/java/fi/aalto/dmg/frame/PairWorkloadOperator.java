package fi.aalto.dmg.frame;

import fi.aalto.dmg.exceptions.WorkloadException;
import fi.aalto.dmg.frame.functions.*;
import fi.aalto.dmg.util.TimeDurations;
import scala.Tuple2;

import java.io.Serializable;

/**
 * Created by yangjun.wang on 24/10/15.
 */
public interface PairWorkloadOperator<K, V> extends Serializable{

    GroupedWorkloadOperator<K,V> groupByKey();

    // TODO: translate to reduce on each node, then group merge
    PairWorkloadOperator<K, V> reduceByKey(ReduceFunction<V> fun, String componentId);

    /**
     * Map <K,V> tuple to <K,R>
     * @param fun map V to R
     * @param componentId component Id of this bolt
     * @param <R>
     * @return maped PairWorkloadOperator<K,R>
     */
    <R> PairWorkloadOperator<K, R> mapValue(MapFunction<V, R> fun, String componentId);

    <R> PairWorkloadOperator<K, R> flatMapValue(FlatMapFunction<V, R> fun, String componentId);

    PairWorkloadOperator<K, V> filter(FilterFunction<Tuple2<K,V>> fun, String componentId);

    PairWorkloadOperator<K, V> updateStateByKey(ReduceFunction<V> fun, String componentId);

    PairWorkloadOperator<K, V> reduceByKeyAndWindow(ReduceFunction<V> fun, String componentId, TimeDurations windowDuration);

    PairWorkloadOperator<K, V> reduceByKeyAndWindow(ReduceFunction<V> fun, String componentId, TimeDurations windowDuration, TimeDurations slideDuration);

    WindowedPairWorkloadOperator<K, V> window(TimeDurations windowDuration);

    WindowedPairWorkloadOperator<K, V> window(TimeDurations windowDuration, TimeDurations slideDuration);

    /**
     * Join two pair streams which have the same type of key -- K
     * @param joinStream
     *          the other stream<K,R>
     * @param windowDuration
     *          window length of this stream
     * @param joinWindowDuration
     *          window length of joinStream
     * @param <R>
     *          Value type of joinStream
     * @return joined stream
     */
    <R> PairWorkloadOperator<K, Tuple2<V,R>> join(
            String componentId, PairWorkloadOperator<K,R> joinStream,
            TimeDurations windowDuration, TimeDurations joinWindowDuration) throws WorkloadException;

    void print();
}

