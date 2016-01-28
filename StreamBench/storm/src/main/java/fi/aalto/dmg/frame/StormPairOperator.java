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
    public PairWorkloadOperator<K, V> reduceByKey(ReduceFunction<V> fun, String componentId, int parallelism, boolean logThroughput) {
        PairReduceBolt<K, V> bolt = new PairReduceBolt<>(fun);
        if(logThroughput){
            bolt.enableThroughput(componentId);
        }
        topologyBuilder.setBolt(componentId, bolt, parallelism)
                .fieldsGrouping(preComponentId, new Fields(BoltConstants.OutputKeyField));
        return new StormPairOperator<>(topologyBuilder, componentId);
    }

    // Set bolt with fieldsGrouping
    @Override
    public PairWorkloadOperator<K, V> reduceByKey(ReduceFunction<V> fun, String componentId, int parallelism) {
        return reduceByKey(fun, componentId, parallelism, false);
    }

    @Override
    public <R> PairWorkloadOperator<K, R> mapValue(MapFunction<V, R> fun, String componentId, int parallelism, boolean logThroughput) {
        MapValueBolt<V, R> bolt = new MapValueBolt<>(fun);
        if(logThroughput){
            bolt.enableThroughput(componentId);
        }
        topologyBuilder.setBolt(componentId, bolt, parallelism)
                .fieldsGrouping(preComponentId, new Fields(BoltConstants.OutputKeyField));
        return new StormPairOperator<>(topologyBuilder, componentId);
    }

    @Override
    public <R> PairWorkloadOperator<K, R> mapValue(MapFunction<V, R> fun, String componentId, int parallelism) {
        return mapValue(fun, componentId, parallelism, false);
    }

    @Override
    public <R> PairWorkloadOperator<K, R> flatMapValue(FlatMapFunction<V, R> fun, String componentId, int parallelism, boolean logThroughput) {
        FlatMapValueBolt<V, R> bolt = new FlatMapValueBolt<>(fun);
        if(logThroughput){
            bolt.enableThroughput(componentId);
        }
        topologyBuilder.setBolt(componentId, bolt, parallelism)
                .fieldsGrouping(preComponentId, new Fields(BoltConstants.OutputKeyField));
        return new StormPairOperator<>(topologyBuilder, componentId);
    }

    @Override
    public <R> PairWorkloadOperator<K, R> flatMapValue(FlatMapFunction<V, R> fun, String componentId, int parallelism) {
        return flatMapValue(fun, componentId, parallelism, false);
    }

    @Override
    public PairWorkloadOperator<K, V> filter(FilterFunction<Tuple2<K, V>> fun, String componentId, int parallelism, boolean logThroughput) {
        PairFilterBolt<K, V> bolt = new PairFilterBolt<>(fun);
        if(logThroughput){
            bolt.enableThroughput(componentId);
        }
        topologyBuilder.setBolt(componentId, bolt, parallelism)
                .fieldsGrouping(preComponentId, new Fields(BoltConstants.OutputKeyField));
        return new StormPairOperator<>(topologyBuilder, componentId);
    }

    @Override
    public PairWorkloadOperator<K, V> filter(FilterFunction<Tuple2<K, V>> fun, String componentId, int parallelism) {
        return filter(fun, componentId, parallelism, false);
    }

    @Override
    public PairWorkloadOperator<K, V> iterative(MapFunction<V, V> mapFunction,
                                                FilterFunction<Tuple2<K, V>> iterativeFunction,
                                                String componentId,
                                                int parallelism) {
        topologyBuilder.setBolt(componentId, new PairIteractiveBolt<>(mapFunction, iterativeFunction))
                .localOrShuffleGrouping(preComponentId)
                .shuffleGrouping(componentId, IteractiveBolt.ITERATIVE_STREAM);
        return new StormPairOperator<>(topologyBuilder, componentId);
    }

    @Override
    public PairWorkloadOperator<K, V> updateStateByKey(ReduceFunction<V> fun, String componentId, int parallelism, boolean logThroughput) {
        return null;
    }

    // Set bolt with fieldsGrouping
    @Override
    public PairWorkloadOperator<K, V> updateStateByKey(ReduceFunction<V> fun, String componentId, int parallelism) {
        return this;
    }

    @Override
    public PairWorkloadOperator<K, V> reduceByKeyAndWindow(ReduceFunction<V> fun, String componentId, int parallelism, TimeDurations windowDuration, boolean logThroughput) {
        return null;
    }

    @Override
    public PairWorkloadOperator<K, V> reduceByKeyAndWindow(ReduceFunction<V> fun, String componentId, int parallelism, TimeDurations windowDuration) {
        return reduceByKeyAndWindow(fun, componentId, parallelism, windowDuration, windowDuration);
    }

    @Override
    public PairWorkloadOperator<K, V> reduceByKeyAndWindow(ReduceFunction<V> fun, String componentId, int parallelism, TimeDurations windowDuration, TimeDurations slideDuration, boolean logThroughput) {
        return null;
    }

    @Override
    public PairWorkloadOperator<K, V> reduceByKeyAndWindow(ReduceFunction<V> fun, String componentId, int parallelism, TimeDurations windowDuration, TimeDurations slideDuration) {
        try {
            topologyBuilder.setBolt(componentId + "-local",
                    new WindowPairReduceByKeyBolt<>(fun, windowDuration, slideDuration),
                    parallelism)
                .localOrShuffleGrouping(preComponentId);
            topologyBuilder.setBolt(componentId,
                    new DiscretizedPairReduceByKeyBolt<>(fun, componentId + "-local"),
                    parallelism)
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
    public <R> PairWorkloadOperator<K, Tuple2<V, R>> join(String componentId, int parallelism,
                                                          PairWorkloadOperator<K, R> joinStream,
                                                          TimeDurations windowDuration,
                                                          TimeDurations joinWindowDuration) throws WorkloadException {

        if(joinStream instanceof StormPairOperator){
            StormPairOperator<K,R> joinStormStream = (StormPairOperator<K,R>)joinStream;
            topologyBuilder.setBolt(componentId,
                    new JoinBolt<>(this.preComponentId, windowDuration, joinStormStream.preComponentId, joinWindowDuration),
                    parallelism)
                .fieldsGrouping(preComponentId, new Fields(BoltConstants.OutputKeyField))
                .fieldsGrouping(joinStormStream.preComponentId, new Fields(BoltConstants.OutputKeyField));
            return new StormPairOperator<>(topologyBuilder, componentId);
        }
        throw new WorkloadException("Cast joinStrem to StormPairOperator failed");
    }

    // Event time join
    @Override
    public <R> PairWorkloadOperator<K, Tuple2<V, R>> join(String componentId, int parallelism, PairWorkloadOperator<K, R> joinStream,
                                                          TimeDurations windowDuration, TimeDurations joinWindowDuration,
                                                          AssignTimeFunction<V> eventTimeAssigner1, AssignTimeFunction<R> eventTimeAssigner2) throws WorkloadException {
        if(joinStream instanceof StormPairOperator){
            StormPairOperator<K,R> joinStormStream = (StormPairOperator<K,R>)joinStream;
            topologyBuilder
                .setBolt(componentId,
                        new JoinBolt<>(this.preComponentId, windowDuration,
                            joinStormStream.preComponentId, joinWindowDuration,
                            eventTimeAssigner1, eventTimeAssigner2),
                        parallelism)
                .fieldsGrouping(preComponentId, new Fields(BoltConstants.OutputKeyField))
                .fieldsGrouping(joinStormStream.preComponentId, new Fields(BoltConstants.OutputKeyField));
            return new StormPairOperator<>(topologyBuilder, componentId);
        }
        throw new WorkloadException("Cast joinStream to StormPairOperator failed");
    }


    @Override
    public void print() {
        topologyBuilder.setBolt("print", new PairPrintBolt<>()).localOrShuffleGrouping(preComponentId);
    }

    @Override
    public void sink(int parallelism) {
        topologyBuilder.setBolt("latency", new PairLatencyBolt<>(), parallelism).localOrShuffleGrouping(preComponentId);

    }

}
