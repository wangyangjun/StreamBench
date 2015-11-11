package fi.aalto.dmg.frame;

import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import fi.aalto.dmg.exceptions.DurationException;
import fi.aalto.dmg.frame.bolts.*;
import fi.aalto.dmg.frame.functions.*;
import fi.aalto.dmg.util.TimeDurations;
import scala.Tuple2;

/**
 * Two types of window stream operator:
 * (1) first time window a stream, it needs window durations and slide durations
 * (2) a windowed stream operator, it receive tick tuple from previous windowed operator
 * Created by jun on 11/9/15.
 */
public class StormWindowedPairWorkloadOperator <K,V> implements WindowedPairWorkloadOperator<K,V>{

    private TopologyBuilder topologyBuilder;
    private String preComponentId;
    private TimeDurations windowDuration;
    private TimeDurations slideDuration;

    /**
     * Constructor for first type window operator
     * @param builder
     * @param previousComponent
     * @param windowDuration
     * @param slideDuration
     */
    public StormWindowedPairWorkloadOperator(TopologyBuilder builder, String previousComponent, TimeDurations windowDuration, TimeDurations slideDuration) {
        this.topologyBuilder = builder;
        this.preComponentId = previousComponent;
        this.windowDuration = windowDuration;
        this.slideDuration = slideDuration;
    }

    /**
     * Constructor for second type window operator
     * @param builder
     * @param previousComponent
     */
    public StormWindowedPairWorkloadOperator(TopologyBuilder builder, String previousComponent) {
        this.topologyBuilder = builder;
        this.preComponentId = previousComponent;
        this.windowDuration = null;
        this.slideDuration = null;
    }


    @Override
    public WindowedPairWorkloadOperator<K, V> reduceByKey(ReduceFunction<V> fun, String componentId) {
        try {
            topologyBuilder.setBolt(componentId, new WindowPairReduceBolt<>(fun, windowDuration, slideDuration))
                    .fieldsGrouping(preComponentId, new Fields(BoltConstants.OutputKeyField));
        } catch (DurationException e) {
            e.printStackTrace();
        }
        return new StormWindowedPairWorkloadOperator<>(topologyBuilder, componentId, windowDuration, slideDuration);
    }

    @Override
    public PairWorkloadOperator<K, V> updateStateByKey(UpdateStateFunction<V> fun, String componentId) {
        topologyBuilder.setBolt(componentId, new UpdateStateBolt<>(fun))
                    .fieldsGrouping(preComponentId, new Fields(BoltConstants.OutputKeyField));
        return new StormPairWordloadOperator<>(topologyBuilder, componentId);
    }

    @Override
    public <R> WindowedPairWorkloadOperator<K, R> mapPartition(MapPartitionFunction<Tuple2<K, V>, Tuple2<K, R>> fun, String componentId) {
        // set bolt

        return new StormWindowedPairWorkloadOperator<>(topologyBuilder, componentId, slideDuration, slideDuration);
    }

    @Override
    public <R> WindowedPairWorkloadOperator<K, R> mapValue(MapFunction<Tuple2<K, V>, Tuple2<K, R>> fun, String componentId) {
        // set bolt

        return new StormWindowedPairWorkloadOperator<>(topologyBuilder, componentId, slideDuration, slideDuration);
    }

    @Override
    public WindowedPairWorkloadOperator<K, V> filter(FilterFunction<Tuple2<K, V>> fun, String componentId) {
        // set bolt

        return new StormWindowedPairWorkloadOperator<>(topologyBuilder, componentId, slideDuration, slideDuration);
    }

    @Override
    public WindowedPairWorkloadOperator<K, V> reduce(ReduceFunction<Tuple2<K, V>> fun, String componentId) {
        // set bolt

        return new StormWindowedPairWorkloadOperator<>(topologyBuilder, componentId, slideDuration, slideDuration);
    }

    @Override
    public void print() {
        topologyBuilder.setBolt("print", new PairPrintBolt<>()).localOrShuffleGrouping(preComponentId);
    }
}
