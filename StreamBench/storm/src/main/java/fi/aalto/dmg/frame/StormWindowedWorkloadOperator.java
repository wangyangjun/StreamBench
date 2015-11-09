package fi.aalto.dmg.frame;

import backtype.storm.topology.TopologyBuilder;
import fi.aalto.dmg.exceptions.DurationException;
import fi.aalto.dmg.frame.bolts.ReduceBolt;
import fi.aalto.dmg.frame.bolts.WindowReduceBolt;
import fi.aalto.dmg.frame.functions.*;
import fi.aalto.dmg.util.TimeDurations;

/**
 * Created by jun on 11/9/15.
 */
public class StormWindowedWorkloadOperator<T> implements WindowedWorkloadOperator<T>  {

    private TopologyBuilder topologyBuilder;
    private String preComponentId;
    private TimeDurations windowDuration;
    private TimeDurations slideDuration;

    public StormWindowedWorkloadOperator(TopologyBuilder builder, String previousComponent, TimeDurations windowDuration, TimeDurations slideDuration) {
        this.topologyBuilder = builder;
        this.preComponentId = previousComponent;
        this.windowDuration = windowDuration;
        this.slideDuration = slideDuration;
    }


    @Override
    public <R> WindowedWorkloadOperator<R> mapPartition(MapPartitionFunction<T, R> fun, String componentId) {
        return null;
    }

    @Override
    public <R> WindowedWorkloadOperator<R> map(MapFunction<T, R> fun, String componentId) {
        return null;
    }

    @Override
    public WindowedWorkloadOperator<T> filter(FilterFunction<T> fun, String componentId) {
        return null;
    }

    @Override
    public WindowedWorkloadOperator<T> reduce(ReduceFunction<T> fun, String componentId) {
        try {
            topologyBuilder.setBolt(componentId, new WindowReduceBolt<>(fun, windowDuration, slideDuration)).localOrShuffleGrouping(preComponentId);
        } catch (DurationException e) {
            e.printStackTrace();
        }
        return new StormWindowedWorkloadOperator<>(topologyBuilder, componentId, windowDuration, slideDuration);
    }

    @Override
    public <K, V> WindowedPairWorkloadOperator<K, V> mapToPair(MapPairFunction<T, K, V> fun, String componentId) {
        return null;
    }

    @Override
    public void print() {

    }
}
