package fi.aalto.dmg.frame;

import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import fi.aalto.dmg.exceptions.DurationException;
import fi.aalto.dmg.frame.bolts.*;
import fi.aalto.dmg.frame.bolts.discretized.DiscretizedPairReduceByKeyBolt;
import fi.aalto.dmg.frame.bolts.windowed.WindowPairReduceByKeyBolt;
import fi.aalto.dmg.frame.functions.*;
import fi.aalto.dmg.util.TimeDurations;
import scala.Tuple2;

/**
 * Created by yangjun.wang on 31/10/15.
 */
public class StormPairOperator<K, V> implements PairWorkloadOperator<K,V> {

    protected TopologyBuilder topologyBuilder;
    protected String preComponentId;

    public StormPairOperator(TopologyBuilder builder, String previousComponent) {
        this.topologyBuilder = builder;
        this.preComponentId = previousComponent;
    }

    @Override
    public GroupedWorkloadOperator<K, V> groupByKey() {
        return new StormGroupedOperator<>(topologyBuilder, this.preComponentId);
    }

    // Set bolt with fieldsGrouping
    @Override
    public PairWorkloadOperator<K, V> reduceByKey(ReduceFunction<V> fun, String componentId) {
        topologyBuilder.setBolt(componentId, new PairReduceBolt<K,V>(fun))
                .fieldsGrouping(preComponentId, new Fields(BoltConstants.OutputKeyField));
        return new StormPairOperator<>(topologyBuilder, componentId);
    }

    @Override
    public <R> PairWorkloadOperator<K, R> mapValue(MapFunction<V, R> fun, String componentId) {
        topologyBuilder.setBolt(componentId, new MapValueBolt<>(fun))
                .localOrShuffleGrouping(preComponentId);
        return new StormPairOperator<>(topologyBuilder, componentId);
    }

    @Override
    public <R> PairWorkloadOperator<K, R> flatMapValue(FlatMapFunction<V, R> fun, String componentId) {
        topologyBuilder.setBolt(componentId, new FlatMapValueBolt<>(fun))
                .localOrShuffleGrouping(preComponentId);
        return new StormPairOperator<>(topologyBuilder, componentId);
    }

    @Override
    public PairWorkloadOperator<K, V> filter(FilterFunction<Tuple2<K, V>> fun, String componentId) {
        topologyBuilder.setBolt(componentId, new PairFilterBolt<K,V>(fun))
                .localOrShuffleGrouping(preComponentId);
        return new StormPairOperator<>(topologyBuilder, componentId);
    }

    // Set bolt with fieldsGrouping
    @Override
    public PairWorkloadOperator<K, V> updateStateByKey(ReduceFunction<V> fun, String componentId) {
        return this;
    }

    @Override
    public WindowedPairWorkloadOperator<K, V> reduceByKeyAndWindow(ReduceFunction<V> fun, String componentId, TimeDurations windowDuration) {
        return reduceByKeyAndWindow(fun, componentId, windowDuration, windowDuration);
    }

    @Override
    public WindowedPairWorkloadOperator<K, V> reduceByKeyAndWindow(ReduceFunction<V> fun, String componentId, TimeDurations windowDuration, TimeDurations slideDuration) {
        try {
            topologyBuilder.setBolt(componentId + "-local", new WindowPairReduceByKeyBolt<>(fun, windowDuration, slideDuration))
                    .localOrShuffleGrouping(preComponentId);
            topologyBuilder.setBolt(componentId, new DiscretizedPairReduceByKeyBolt<>(fun, componentId + "-local"))
                    .fieldsGrouping(componentId + "-local", new Fields(BoltConstants.OutputKeyField))
                    .allGrouping(componentId + "-local", BoltConstants.TICK_STREAM_ID);
        } catch (DurationException e) {
            e.printStackTrace();
        }
        return new StormDiscretizedPairOperator<>(topologyBuilder, componentId);
    }

    @Override
    public WindowedPairWorkloadOperator<K, V> window(TimeDurations windowDuration) {
        return window(windowDuration, windowDuration);
    }

    @Override
    public WindowedPairWorkloadOperator<K, V> window(TimeDurations windowDuration, TimeDurations slideDuration) {
        return new StormWindowedPairOperator<>(topologyBuilder, preComponentId, windowDuration, slideDuration);
    }


    @Override
    public void print() {
        topologyBuilder.setBolt("print", new PairPrintBolt<>()).localOrShuffleGrouping(preComponentId);
    }

}
