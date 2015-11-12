package fi.aalto.dmg.frame;

import backtype.storm.topology.TopologyBuilder;
import fi.aalto.dmg.frame.functions.*;
import scala.Tuple2;

/**
 * Created by jun on 11/12/15.
 */
public class StormDiscretizedPairOperator<K,V> implements WindowedPairWorkloadOperator<K,V> {

    private TopologyBuilder topologyBuilder;
    private String preComponentId;

    public StormDiscretizedPairOperator(TopologyBuilder builder, String previousComponent) {
        this.topologyBuilder = builder;
        this.preComponentId = previousComponent;
    }

    @Override
    public WindowedPairWorkloadOperator<K, V> reduceByKey(ReduceFunction<V> fun, String componentId) {
        return null;
    }

    @Override
    public PairWorkloadOperator<K, V> updateStateByKey(UpdateStateFunction<V> fun, String componentId) {
        return null;
    }

    @Override
    public <R> WindowedPairWorkloadOperator<K, R> mapPartition(MapPartitionFunction<Tuple2<K, V>, Tuple2<K, R>> fun, String componentId) {
        return null;
    }

    @Override
    public <R> WindowedPairWorkloadOperator<K, R> mapValue(MapFunction<Tuple2<K, V>, Tuple2<K, R>> fun, String componentId) {
        return null;
    }

    @Override
    public WindowedPairWorkloadOperator<K, V> filter(FilterFunction<Tuple2<K, V>> fun, String componentId) {
        return null;
    }

    @Override
    public WindowedPairWorkloadOperator<K, V> reduce(ReduceFunction<Tuple2<K, V>> fun, String componentId) {
        return null;
    }

    @Override
    public void print() {

    }
}
