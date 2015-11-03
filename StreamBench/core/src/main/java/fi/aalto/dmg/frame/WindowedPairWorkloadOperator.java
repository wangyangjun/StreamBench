package fi.aalto.dmg.frame;

import fi.aalto.dmg.frame.functions.*;
import scala.Tuple2;

import java.io.Serializable;

/**
 * Created by jun on 11/3/15.
 */
public interface WindowedPairWorkloadOperator<K,V> extends WindowedWorkloadOperator<Tuple2<K,V>>, Serializable {

    GroupedWorkloadOperator<K,V> groupByKey();

    // TODO: translate to reduce on each node, then group merge
    PairWorkloadOperator<K, V> reduceByKey(ReduceFunction<V> fun, String componentId);

    PairWorkloadOperator<K, V> updateStateByKey(UpdateStateFunction<V> fun, String componentId);

    // TODO: new APIs filter, mapValue
}
