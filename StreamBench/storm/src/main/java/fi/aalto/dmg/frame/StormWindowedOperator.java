package fi.aalto.dmg.frame;

import backtype.storm.topology.TopologyBuilder;
import fi.aalto.dmg.exceptions.DurationException;
import fi.aalto.dmg.frame.bolts.PrintBolt;
import fi.aalto.dmg.frame.bolts.windowed.*;
import fi.aalto.dmg.frame.functions.*;
import fi.aalto.dmg.util.TimeDurations;
import org.apache.log4j.Logger;


/**
 * Created by jun on 11/9/15.
 */
public class StormWindowedOperator<T> implements WindowedWorkloadOperator<T>  {

    private static final long serialVersionUID = -6227716871223564255L;
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
    public <R> WorkloadOperator<R> mapPartition(MapPartitionFunction<T, R> fun, String componentId, boolean logThroughput) {
        try {
            if(logThroughput) {
                topologyBuilder.setBolt(componentId,
                            new WindowMapPartitionBolt<>(fun, windowDuration, slideDuration,
                            Logger.getLogger(componentId)))
                        .localOrShuffleGrouping(preComponentId);
            } else {
                topologyBuilder.setBolt(componentId, new WindowMapPartitionBolt<>(fun, windowDuration, slideDuration))
                        .localOrShuffleGrouping(preComponentId);
            }
        } catch (DurationException e) {
            e.printStackTrace();
        }
        return new StormOperator<>(topologyBuilder, componentId);
    }

    @Override
    public <R> WorkloadOperator<R> mapPartition(MapPartitionFunction<T, R> fun, String componentId) {
        return mapPartition(fun, componentId, false);
    }

    @Override
    public <R> WorkloadOperator<R> map(MapFunction<T, R> fun, String componentId, boolean logThroughput) {
        try {
            if(logThroughput) {
                topologyBuilder.setBolt(componentId, new WindowMapBolt<>(fun, windowDuration, slideDuration, Logger.getLogger(componentId)))
                        .localOrShuffleGrouping(preComponentId);
            } else {
                topologyBuilder.setBolt(componentId, new WindowMapBolt<>(fun, windowDuration, slideDuration))
                        .localOrShuffleGrouping(preComponentId);
            }
        } catch (DurationException e) {
            e.printStackTrace();
        }
        return new StormOperator<>(topologyBuilder, componentId);
    }

    @Override
    public <R> WorkloadOperator<R> map(MapFunction<T, R> fun, String componentId) {
        return map(fun, componentId, false);
    }

    @Override
    public WorkloadOperator<T> filter(FilterFunction<T> fun, String componentId, boolean logThroughput) {
        try {
            if(logThroughput) {
                topologyBuilder.setBolt(componentId, new WindowFilterBolt<>(fun, windowDuration, slideDuration, Logger.getLogger(componentId)))
                        .localOrShuffleGrouping(preComponentId);
            } else {
                topologyBuilder.setBolt(componentId, new WindowFilterBolt<>(fun, windowDuration, slideDuration))
                        .localOrShuffleGrouping(preComponentId);
            }
        } catch (DurationException e) {
            e.printStackTrace();
        }
        return new StormOperator<>(topologyBuilder, componentId);
    }

    @Override
    public WorkloadOperator<T> filter(FilterFunction<T> fun, String componentId) {
        return filter(fun, componentId, false);
    }

    @Override
    public WorkloadOperator<T> reduce(ReduceFunction<T> fun, String componentId, boolean logThroughput) {
        try {
            if(logThroughput){
                topologyBuilder.setBolt(componentId, new WindowReduceBolt<>(fun, windowDuration, slideDuration, Logger.getLogger(componentId)))
                        .localOrShuffleGrouping(preComponentId);
            } else {
                topologyBuilder.setBolt(componentId, new WindowReduceBolt<>(fun, windowDuration, slideDuration))
                        .localOrShuffleGrouping(preComponentId);
            }
        } catch (DurationException e) {
            e.printStackTrace();
        }
        return new StormOperator<>(topologyBuilder, componentId);
    }

    @Override
    public WorkloadOperator<T> reduce(ReduceFunction<T> fun, String componentId) {
        return reduce(fun, componentId, false);
    }

    @Override
    public <K, V> PairWorkloadOperator<K, V> mapToPair(MapPairFunction<T, K, V> fun, String componentId, boolean logThroughput) {
        try {
            if(logThroughput) {
                topologyBuilder.setBolt(componentId, new WindowMapToPairBolt<>(fun, windowDuration, slideDuration, Logger.getLogger(componentId)))
                        .localOrShuffleGrouping(preComponentId);

            } else {
                topologyBuilder.setBolt(componentId, new WindowMapToPairBolt<>(fun, windowDuration, slideDuration))
                        .localOrShuffleGrouping(preComponentId);
            }
        } catch (DurationException e) {
            e.printStackTrace();
        }
        return new StormPairOperator<>(topologyBuilder, componentId);
    }

    @Override
    public <K, V> PairWorkloadOperator<K, V> mapToPair(MapPairFunction<T, K, V> fun, String componentId) {
        return mapToPair(fun, componentId, false);
    }

    @Override
    public void print() {
        topologyBuilder.setBolt("print", new PrintBolt<>(true)).localOrShuffleGrouping(preComponentId);
    }
}
