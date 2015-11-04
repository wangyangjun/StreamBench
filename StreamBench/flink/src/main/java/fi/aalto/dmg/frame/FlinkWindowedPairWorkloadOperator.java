package fi.aalto.dmg.frame;

import fi.aalto.dmg.frame.functions.MapPartitionFunction;
import fi.aalto.dmg.frame.functions.ReduceFunction;
import fi.aalto.dmg.frame.functions.UpdateStateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedDataStream;
import org.apache.flink.streaming.api.functions.WindowMapFunction;
import org.apache.flink.util.Collector;
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

    @Override
    public <R> PairWorkloadOperator<K, R> mapPartitionToPair(final MapPartitionFunction<Tuple2<K, V>, Tuple2<K, R>> fun, String componentId) {
        DataStream<Tuple2<K,R>> newDataStream = dataStream.mapWindow(new WindowMapFunction<Tuple2<K,V>, Tuple2<K,R>>() {
            @Override
            public void mapWindow(Iterable<Tuple2<K, V>> values, Collector<Tuple2<K, R>> collector) throws Exception {
                Iterable<Tuple2<K,R>> results = fun.mapPartition(values);
                for (Tuple2<K,R> r : results) {
                    collector.collect(r);
                }
            }
        }).flatten();
        return new FlinkPairWorkloadOperator<>(newDataStream);
    }
}
