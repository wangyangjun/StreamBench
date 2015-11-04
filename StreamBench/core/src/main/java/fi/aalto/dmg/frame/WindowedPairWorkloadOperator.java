package fi.aalto.dmg.frame;

import fi.aalto.dmg.frame.functions.*;
import scala.Tuple2;

import java.io.Serializable;

/**
 * Created by jun on 11/3/15.
 */
public interface WindowedPairWorkloadOperator<K,V> extends WindowedWorkloadOperator<Tuple2<K,V>>, Serializable {

    /**
     * Spark groupByKey return <K, Iterable<V>>, avoid use groupByKey
     * */
    GroupedWorkloadOperator<K,V> groupByKey();

    /**
     *
     * @param fun
     * @param componentId
     * @return PairWorkloadOperator<K, V>
     */
    PairWorkloadOperator<K, V> reduceByKey(ReduceFunction<V> fun, String componentId);

    /**
     * TODO: updateStateByKey after window
     * @param fun
     * @param componentId
     * @return
     */
    PairWorkloadOperator<K, V> updateStateByKey(UpdateStateFunction<V> fun, String componentId);

    // return WorkloadOperator<R>
    <R> PairWorkloadOperator<K, R> mapPartitionToPair(MapPartitionFunction<Tuple2<K,V>, Tuple2<K,R>> fun, String componentId);

    // TODO: new APIs filter, mapValue
}
