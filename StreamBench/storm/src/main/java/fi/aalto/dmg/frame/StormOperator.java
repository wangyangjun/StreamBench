package fi.aalto.dmg.frame;

import backtype.storm.topology.TopologyBuilder;
import fi.aalto.dmg.frame.bolts.*;
import fi.aalto.dmg.frame.functions.*;
import fi.aalto.dmg.util.TimeDurations;

/**
 * Created by yangjun.wang on 31/10/15.
 */
public class StormOperator<T> extends OperatorBase implements WorkloadOperator<T> {
    protected TopologyBuilder topologyBuilder;
    protected String preComponentId;

    public StormOperator(TopologyBuilder builder, String previousComponent){
        this.topologyBuilder = builder;
        this.preComponentId = previousComponent;
    }
    @Override
    public <R> WorkloadOperator<R> map(MapFunction<T, R> fun, String componentId) {
        topologyBuilder.setBolt(componentId, new MapBolt<>(fun)).localOrShuffleGrouping(preComponentId);
        return new StormOperator<>(topologyBuilder, componentId);
    }

    @Override
    public <K, V> PairWorkloadOperator<K, V> mapToPair(MapPairFunction<T, K, V> fun, String componentId) {
        topologyBuilder.setBolt(componentId, new MapToPairBolt<>(fun)).localOrShuffleGrouping(preComponentId);
        return new StormPairOperator<>(topologyBuilder, componentId);
    }

    // TODO: whether previous operation is group
    @Override
    public WorkloadOperator<T> reduce(ReduceFunction<T> fun, String componentId) {
        topologyBuilder.setBolt(componentId, new ReduceBolt<>(fun)).localOrShuffleGrouping(preComponentId);
        return new StormOperator<>(topologyBuilder, componentId);
    }

    @Override
    public WorkloadOperator<T> filter(FilterFunction<T> fun, String componentId) {
        topologyBuilder.setBolt(componentId, new FilterBolt<>(fun)).localOrShuffleGrouping(preComponentId);
        return new StormOperator<>(topologyBuilder, componentId);
    }

    @Override
    public <R> WorkloadOperator<R> flatMap(FlatMapFunction<T, R> fun, String componentId) {
        topologyBuilder.setBolt(componentId, new FlatMapBolt<>(fun)).localOrShuffleGrouping(preComponentId);
        return new StormOperator<>(topologyBuilder, componentId);
    }

    @Override
    public WindowedWorkloadOperator<T> window(TimeDurations windowDuration) {
        return window(windowDuration, windowDuration);
    }

    @Override
    public WindowedWorkloadOperator<T> window(TimeDurations windowDuration, TimeDurations slideDuration) {
        return new StormWindowedOperator<T>(topologyBuilder, preComponentId, windowDuration, slideDuration);
    }

    @Override
    public void print() {
        topologyBuilder.setBolt("print", new PrintBolt<T>()).localOrShuffleGrouping(preComponentId);
    }
}
