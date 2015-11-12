package fi.aalto.dmg.frame;

import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import fi.aalto.dmg.frame.bolts.BoltConstants;
import fi.aalto.dmg.frame.bolts.discretized.DiscretizedMapBolt;
import fi.aalto.dmg.frame.bolts.discretized.DiscretizedPairReduceBolt;
import fi.aalto.dmg.frame.functions.*;
import fi.aalto.dmg.util.TimeDurations;

/**
 * Created by jun on 11/12/15.
 */
public class StormDiscretizedOperator<T> implements WindowedWorkloadOperator<T> {

    protected TopologyBuilder topologyBuilder;
    protected String preComponentId;

    public StormDiscretizedOperator(TopologyBuilder builder, String previousComponent) {
        this.topologyBuilder = builder;
        this.preComponentId = previousComponent;
    }


    @Override
    public <R> WindowedWorkloadOperator<R> mapPartition(MapPartitionFunction<T, R> fun, String componentId) {
        return null;
    }

    @Override
    public <R> WindowedWorkloadOperator<R> map(MapFunction<T, R> fun, String componentId) {
        topologyBuilder.setBolt(componentId, new DiscretizedMapBolt<>(fun, preComponentId))
                .fieldsGrouping(preComponentId, new Fields(BoltConstants.OutputKeyField))
                .allGrouping(preComponentId, BoltConstants.TICK_STREAM_ID);
        return new StormDiscretizedOperator<>(topologyBuilder, componentId);
    }

    @Override
    public WindowedWorkloadOperator<T> filter(FilterFunction<T> fun, String componentId) {
        return null;
    }

    @Override
    public WindowedWorkloadOperator<T> reduce(ReduceFunction<T> fun, String componentId) {
        return null;
    }

    @Override
    public <K, V> WindowedPairWorkloadOperator<K, V> mapToPair(MapPairFunction<T, K, V> fun, String componentId) {
        return null;
    }

    @Override
    public void print() {

    }
}
