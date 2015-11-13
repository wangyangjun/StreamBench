package fi.aalto.dmg.frame;

import backtype.storm.topology.TopologyBuilder;
import fi.aalto.dmg.exceptions.DurationException;
import fi.aalto.dmg.frame.bolts.windowed.*;
import fi.aalto.dmg.frame.functions.*;
import fi.aalto.dmg.util.TimeDurations;

/**
 * Created by jun on 11/9/15.
 */
public class StormWindowedOperator<T> implements WindowedWorkloadOperator<T>  {

    protected TopologyBuilder topologyBuilder;
    protected String preComponentId;
    private TimeDurations windowDuration;
    private TimeDurations slideDuration;

    public StormWindowedOperator(TopologyBuilder builder, String previousComponent) {
        this.topologyBuilder = builder;
        this.preComponentId = previousComponent;
    }

    public StormWindowedOperator(TopologyBuilder builder, String previousComponent, TimeDurations windowDuration, TimeDurations slideDuration) {
        this.topologyBuilder = builder;
        this.preComponentId = previousComponent;
        this.windowDuration = windowDuration;
        this.slideDuration = slideDuration;
    }


    @Override
    public <R> WindowedWorkloadOperator<R> mapPartition(MapPartitionFunction<T, R> fun, String componentId) {
        try {
            topologyBuilder.setBolt(componentId, new WindowMapPartitionBolt<>(fun, windowDuration, slideDuration)).localOrShuffleGrouping(preComponentId);
        } catch (DurationException e) {
            e.printStackTrace();
        }
        return new StormDiscretizedOperator<>(topologyBuilder, componentId);
    }

    @Override
    public <R> WindowedWorkloadOperator<R> map(MapFunction<T, R> fun, String componentId) {
        try {
            topologyBuilder.setBolt(componentId, new WindowMapBolt<>(fun, windowDuration, slideDuration)).localOrShuffleGrouping(preComponentId);
        } catch (DurationException e) {
            e.printStackTrace();
        }
        return new StormDiscretizedOperator<>(topologyBuilder, componentId);
    }

    @Override
    public WindowedWorkloadOperator<T> filter(FilterFunction<T> fun, String componentId) {
        try {
            topologyBuilder.setBolt(componentId, new WindowFilterBolt<>(fun, windowDuration, slideDuration)).localOrShuffleGrouping(preComponentId);
        } catch (DurationException e) {
            e.printStackTrace();
        }
        return new StormDiscretizedOperator<>(topologyBuilder, componentId);
    }

    @Override
    public WindowedWorkloadOperator<T> reduce(ReduceFunction<T> fun, String componentId) {
        try {
            topologyBuilder.setBolt(componentId, new WindowReduceBolt<>(fun, windowDuration, slideDuration)).localOrShuffleGrouping(preComponentId);
        } catch (DurationException e) {
            e.printStackTrace();
        }
        return new StormDiscretizedOperator<>(topologyBuilder, componentId);
    }

    @Override
    public <K, V> WindowedPairWorkloadOperator<K, V> mapToPair(MapPairFunction<T, K, V> fun, String componentId) {
        try {
            topologyBuilder.setBolt(componentId, new WindowMapToPairBolt<>(fun, windowDuration, slideDuration)).localOrShuffleGrouping(preComponentId);
        } catch (DurationException e) {
            e.printStackTrace();
        }
        return new StormDiscretizedPairOperator<>(topologyBuilder, componentId);
    }

    @Override
    public void print() {

    }
}
