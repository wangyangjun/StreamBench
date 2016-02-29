package fi.aalto.dmg.frame;

import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import fi.aalto.dmg.exceptions.DurationException;
import fi.aalto.dmg.exceptions.UnsupportOperatorException;
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
public class StormPairOperator<K, V> extends PairWorkloadOperator<K,V> {

    private static final long serialVersionUID = -495651940753255655L;
    protected TopologyBuilder topologyBuilder;
    protected String preComponentId;

    public StormPairOperator(TopologyBuilder builder, String previousComponent, int parallelism) {
        super(parallelism);
        this.topologyBuilder = builder;
        this.preComponentId = previousComponent;
    }

    @Override
    public GroupedWorkloadOperator<K, V> groupByKey() {
        return new StormGroupedOperator<>(topologyBuilder, this.preComponentId, parallelism);
    }

    @Override
    public PairWorkloadOperator<K, V> reduceByKey(ReduceFunction<V> fun, String componentId, boolean logThroughput) {
        PairReduceBolt<K, V> bolt = new PairReduceBolt<>(fun);
        if(logThroughput){
            bolt.enableThroughput(componentId);
        }
        topologyBuilder.setBolt(componentId, bolt, parallelism)
                .fieldsGrouping(preComponentId, new Fields(BoltConstants.OutputKeyField));
        return new StormPairOperator<>(topologyBuilder, componentId, parallelism);
    }

    // Set bolt with fieldsGrouping
    @Override
    public PairWorkloadOperator<K, V> reduceByKey(ReduceFunction<V> fun, String componentId) {
        return reduceByKey(fun, componentId, false);
    }

    @Override
    public <R> PairWorkloadOperator<K, R> mapValue(MapFunction<V, R> fun, String componentId, boolean logThroughput) {
        MapValueBolt<V, R> bolt = new MapValueBolt<>(fun);
        if(logThroughput){
            bolt.enableThroughput(componentId);
        }
        topologyBuilder.setBolt(componentId, bolt, parallelism)
                .fieldsGrouping(preComponentId, new Fields(BoltConstants.OutputKeyField));
        return new StormPairOperator<>(topologyBuilder, componentId, parallelism);
    }

    @Override
    public <R> PairWorkloadOperator<K, R> mapValue(MapFunction<V, R> fun, String componentId) {
        return mapValue(fun, componentId, false);
    }

    @Override
    public <R> WorkloadOperator<R> map(MapFunction<Tuple2<K, V>, R> fun, String componentId, boolean logThroughput) {
        PairMapBolt<K, V, R> bolt = new PairMapBolt<>(fun);
        if(logThroughput){
            bolt.enableThroughput(componentId);
        }
        topologyBuilder.setBolt(componentId, bolt, parallelism)
                .fieldsGrouping(preComponentId, new Fields(BoltConstants.OutputKeyField));
        return new StormOperator<>(topologyBuilder, componentId, parallelism);
    }

    @Override
    public <R> WorkloadOperator<R> map(MapFunction<Tuple2<K, V>, R> fun, String componentId) {
        return map(fun, componentId, false);
    }

    @Override
    public <R> WorkloadOperator<R> map(MapFunction<Tuple2<K, V>, R> fun, String componentId, Class<R> outputClass) {
        return map(fun, componentId, outputClass, false);
    }

    @Override
    public <R> WorkloadOperator<R> map(MapFunction<Tuple2<K, V>, R> fun, String componentId, Class<R> outputClass, boolean logThroughput) {
        return map(fun, componentId, logThroughput);
    }

    @Override
    public <R> PairWorkloadOperator<K, R> flatMapValue(FlatMapFunction<V, R> fun, String componentId, boolean logThroughput) {
        FlatMapValueBolt<V, R> bolt = new FlatMapValueBolt<>(fun);
        if(logThroughput){
            bolt.enableThroughput(componentId);
        }
        topologyBuilder.setBolt(componentId, bolt, parallelism)
                .fieldsGrouping(preComponentId, new Fields(BoltConstants.OutputKeyField));
        return new StormPairOperator<>(topologyBuilder, componentId, parallelism);
    }

    @Override
    public <R> PairWorkloadOperator<K, R> flatMapValue(FlatMapFunction<V, R> fun, String componentId) {
        return flatMapValue(fun, componentId, false);
    }

    @Override
    public PairWorkloadOperator<K, V> filter(FilterFunction<Tuple2<K, V>> fun, String componentId, boolean logThroughput) {
        PairFilterBolt<K, V> bolt = new PairFilterBolt<>(fun);
        if(logThroughput){
            bolt.enableThroughput(componentId);
        }
        topologyBuilder.setBolt(componentId, bolt, parallelism)
                .fieldsGrouping(preComponentId, new Fields(BoltConstants.OutputKeyField));
        return new StormPairOperator<>(topologyBuilder, componentId, parallelism);
    }

    @Override
    public PairWorkloadOperator<K, V> filter(FilterFunction<Tuple2<K, V>> fun, String componentId) {
        return filter(fun, componentId, false);
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
        return new StormPairOperator<>(topologyBuilder, componentId, parallelism);
    }

    @Override
    public WindowedPairWorkloadOperator<K, V> window(TimeDurations windowDuration) {
        return window(windowDuration, windowDuration);
    }

    @Override
    public WindowedPairWorkloadOperator<K, V> window(TimeDurations windowDuration, TimeDurations slideDuration) {
        return new StormWindowedPairOperator<>(topologyBuilder, preComponentId, windowDuration, slideDuration, parallelism);
    }

    @Override
    public <R> PairWorkloadOperator<K, Tuple2<V, R>> join(String componentId,
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
            return new StormPairOperator<>(topologyBuilder, componentId, parallelism);
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
                .setBolt(componentId,
                        new JoinBolt<>(this.preComponentId, windowDuration,
                            joinStormStream.preComponentId, joinWindowDuration,
                            eventTimeAssigner1, eventTimeAssigner2),
                        parallelism)
                .fieldsGrouping(preComponentId, new Fields(BoltConstants.OutputKeyField))
                .fieldsGrouping(joinStormStream.preComponentId, new Fields(BoltConstants.OutputKeyField));
            return new StormPairOperator<>(topologyBuilder, componentId, parallelism);
        }
        throw new WorkloadException("Cast joinStream to StormPairOperator failed");
    }


    @Override
    public void closeWith(OperatorBase stream, boolean broadcast) throws UnsupportOperatorException {
        throw new UnsupportOperatorException("not implemented yet");
    }

    @Override
    public void print() {
        topologyBuilder.setBolt("print", new PairPrintBolt<>()).localOrShuffleGrouping(preComponentId);
    }

    @Override
    public void sink() {
        topologyBuilder.setBolt("latency", new PairLatencyBolt<>(), parallelism).localOrShuffleGrouping(preComponentId);

    }

}
