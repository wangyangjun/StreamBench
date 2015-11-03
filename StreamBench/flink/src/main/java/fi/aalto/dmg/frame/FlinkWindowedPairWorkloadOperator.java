package fi.aalto.dmg.frame;

import fi.aalto.dmg.frame.functions.ReduceFunction;
import fi.aalto.dmg.frame.functions.UpdateStateFunction;
import org.apache.flink.streaming.api.datastream.WindowedDataStream;
import scala.Tuple2;

/**
 * Created by jun on 11/3/15.
 */
public class FlinkWindowedPairWorkloadOperator<K,V>
        extends FlinkWindowedWorkloadOperator<Tuple2<K,V>> implements WindowedPairWorkloadOperator<K,V>{
    public FlinkWindowedPairWorkloadOperator(WindowedDataStream<Tuple2<K, V>> dataStream1) {
        super(dataStream1);
    }

    @Override
    public GroupedWorkloadOperator<K, V> groupByKey() {
        return null;
    }

    @Override
    public PairWorkloadOperator<K, V> reduceByKey(ReduceFunction<V> fun, String componentId) {
        return null;
    }

    @Override
    public PairWorkloadOperator<K, V> updateStateByKey(UpdateStateFunction<V> fun, String componentId) {
        return null;
    }
}
