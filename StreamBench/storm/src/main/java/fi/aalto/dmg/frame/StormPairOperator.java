package fi.aalto.dmg.frame;

import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import fi.aalto.dmg.exceptions.DurationException;
import fi.aalto.dmg.exceptions.WorkloadException;
import fi.aalto.dmg.frame.bolts.*;
import fi.aalto.dmg.frame.bolts.discretized.DiscretizedPairReduceByKeyBolt;
import fi.aalto.dmg.frame.bolts.windowed.WindowPairReduceByKeyBolt;
import fi.aalto.dmg.frame.functions.*;
import fi.aalto.dmg.util.TimeDurations;
import org.apache.log4j.Logger;
import scala.Tuple2;

/**
 * Created by yangjun.wang on 31/10/15.
 */
public class StormPairOperator<K, V> implements PairWorkloadOperator<K,V> {

    private static final long serialVersionUID = -495651940753255655L;
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

    @Override
    public PairWorkloadOperator<K, V> reduceByKey(ReduceFunction<V> fun, String componentId, boolean logThroughput) {
        if(logThroughput){
            topologyBuilder.setBolt(componentId, new PairReduceBolt<K, V>(fun, Logger.getLogger(componentId)))
                    .fieldsGrouping(preComponentId, new Fields(BoltConstants.OutputKeyField));
        } else {
            topologyBuilder.setBolt(componentId, new PairReduceBolt<K, V>(fun))
                    .fieldsGrouping(preComponentId, new Fields(BoltConstants.OutputKeyField));
        }
        return new StormPairOperator<>(topologyBuilder, componentId);
    }

    // Set bolt with fieldsGrouping
    @Override
    public PairWorkloadOperator<K, V> reduceByKey(ReduceFunction<V> fun, String componentId) {
        return reduceByKey(fun, componentId, false);
    }

    @Override
    public <R> PairWorkloadOperator<K, R> mapValue(MapFunction<V, R> fun, String componentId, boolean logThroughput) {
        if(logThroughput){
            topologyBuilder.setBolt(componentId, new MapValueBolt<>(fun, Logger.getLogger(componentId)))
                    .localOrShuffleGrouping(preComponentId);
        } else {
            topologyBuilder.setBolt(componentId, new MapValueBolt<>(fun))
                    .localOrShuffleGrouping(preComponentId);
        }
        return new StormPairOperator<>(topologyBuilder, componentId);
    }

    @Override
    public <R> PairWorkloadOperator<K, R> mapValue(MapFunction<V, R> fun, String componentId) {
        return mapValue(fun, componentId, false);
    }

    @Override
    public <R> PairWorkloadOperator<K, R> flatMapValue(FlatMapFunction<V, R> fun, String componentId, boolean logThroughput) {
        if(logThroughput){
            topologyBuilder.setBolt(componentId, new FlatMapValueBolt<>(fun, Logger.getLogger(componentId)))
                    .localOrShuffleGrouping(preComponentId);
        } else {
            topologyBuilder.setBolt(componentId, new FlatMapValueBolt<>(fun))
                    .localOrShuffleGrouping(preComponentId);
        }
        return new StormPairOperator<>(topologyBuilder, componentId);
    }

    @Override
    public <R> PairWorkloadOperator<K, R> flatMapValue(FlatMapFunction<V, R> fun, String componentId) {
        return flatMapValue(fun, componentId, false);
    }

    @Override
    public PairWorkloadOperator<K, V> filter(FilterFunction<Tuple2<K, V>> fun, String componentId, boolean logThroughput) {
        if(logThroughput){
            topologyBuilder.setBolt(componentId, new PairFilterBolt<K,V>(fun, Logger.getLogger(componentId)))
                    .localOrShuffleGrouping(preComponentId);
        } else {
            topologyBuilder.setBolt(componentId, new PairFilterBolt<K,V>(fun))
                    .localOrShuffleGrouping(preComponentId);
        }
        return new StormPairOperator<>(topologyBuilder, componentId);
    }

    @Override
    public PairWorkloadOperator<K, V> filter(FilterFunction<Tuple2<K, V>> fun, String componentId) {
        return filter(fun, componentId, false);
    }

    @Override
    public PairWorkloadOperator<K, V> iterative(MapFunction<V, V> mapFunction, FilterFunction<Tuple2<K, V>> iterativeFunction, String componentId) {
        topologyBuilder.setBolt(componentId, new PairIteractiveBolt<>(mapFunction, iterativeFunction))
                .localOrShuffleGrouping(preComponentId)
                .shuffleGrouping(componentId, IteractiveBolt.ITERATIVE_STREAM);
        return new StormPairOperator<>(topologyBuilder, componentId);
    }

    @Override
    public PairWorkloadOperator<K, V> updateStateByKey(ReduceFunction<V> fun, String componentId, boolean logThroughput) {
        return null;
    }

    // Set bolt with fieldsGrouping
    @Override
    public PairWorkloadOperator<K, V> updateStateByKey(ReduceFunction<V> fun, String componentId) {
        return this;
    }

    @Override
    public PairWorkloadOperator<K, V> reduceByKeyAndWindow(ReduceFunction<V> fun, String componentId, TimeDurations windowDuration, boolean logThroughput) {
        return null;
    }

    @Override
    public PairWorkloadOperator<K, V> reduceByKeyAndWindow(ReduceFunction<V> fun, String componentId, TimeDurations windowDuration) {
        return reduceByKeyAndWindow(fun, componentId, windowDuration, windowDuration);
    }

    @Override
    public PairWorkloadOperator<K, V> reduceByKeyAndWindow(ReduceFunction<V> fun, String componentId, TimeDurations windowDuration, TimeDurations slideDuration, boolean logThroughput) {
        return null;
    }

    @Override
    public PairWorkloadOperator<K, V> reduceByKeyAndWindow(ReduceFunction<V> fun, String componentId, TimeDurations windowDuration, TimeDurations slideDuration) {
        try {
            topologyBuilder.setBolt(componentId + "-local", new WindowPairReduceByKeyBolt<>(fun, windowDuration, slideDuration))
                    .localOrShuffleGrouping(preComponentId);
            topologyBuilder.setBolt(componentId, new DiscretizedPairReduceByKeyBolt<>(fun, componentId + "-local"))
                    .fieldsGrouping(componentId + "-local", new Fields(BoltConstants.OutputKeyField))
                    .allGrouping(componentId + "-local", BoltConstants.TICK_STREAM_ID);
        } catch (DurationException e) {
            e.printStackTrace();
        }
        return new StormPairOperator<>(topologyBuilder, componentId);
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
    public <R> PairWorkloadOperator<K, Tuple2<V, R>> join(String componentId, PairWorkloadOperator<K, R> joinStream, TimeDurations windowDuration, TimeDurations joinWindowDuration) throws WorkloadException {

        if(joinStream instanceof StormPairOperator){
            StormPairOperator<K,R> joinStormStream = (StormPairOperator<K,R>)joinStream;
            topologyBuilder.setBolt(componentId, new JoinBolt<>(componentId, windowDuration, joinStormStream.preComponentId, joinWindowDuration))
                    .fieldsGrouping(preComponentId, new Fields(BoltConstants.OutputKeyField))
                    .fieldsGrouping(joinStormStream.preComponentId, new Fields(BoltConstants.OutputKeyField));
            return new StormPairOperator<>(topologyBuilder, componentId);
        }
        throw new WorkloadException("Cast joinStrem to StormPairOperator failed");
    }

    // Event time join
    @Override
    public <R> PairWorkloadOperator<K, Tuple2<V, R>> join(String componentId, PairWorkloadOperator<K, R> joinStream,
                                                          TimeDurations windowDuration, TimeDurations joinWindowDuration,
                                                          AssignTimeFunction<V> eventTimeAssigner1, AssignTimeFunction<R> eventTimeAssigner2) throws WorkloadException {
        if(joinStream instanceof StormPairOperator){
            StormPairOperator<K,R> joinStormStream = (StormPairOperator<K,R>)joinStream;
            topologyBuilder
                .setBolt(componentId, new JoinBolt<>(componentId, windowDuration,
                    joinStormStream.preComponentId, joinWindowDuration,
                    eventTimeAssigner1, eventTimeAssigner2))
                .fieldsGrouping(preComponentId, new Fields(BoltConstants.OutputKeyField))
                .fieldsGrouping(joinStormStream.preComponentId, new Fields(BoltConstants.OutputKeyField));
            return new StormPairOperator<>(topologyBuilder, componentId);
        }
        throw new WorkloadException("Cast joinStrem to StormPairOperator failed");
    }


    @Override
    public void print() {
        topologyBuilder.setBolt("print", new PairPrintBolt<>()).localOrShuffleGrouping(preComponentId);
    }

    @Override
    public void sink() {
        topologyBuilder.setBolt("latency", new PairLatencyBolt<>()).localOrShuffleGrouping(preComponentId);

    }

}
