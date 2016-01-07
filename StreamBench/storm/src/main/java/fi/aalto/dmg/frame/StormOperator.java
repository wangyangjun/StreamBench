package fi.aalto.dmg.frame;

import backtype.storm.topology.TopologyBuilder;
import fi.aalto.dmg.frame.bolts.*;
import fi.aalto.dmg.frame.functions.*;
import fi.aalto.dmg.util.TimeDurations;
import org.apache.log4j.Logger;

/**
 * Created by yangjun.wang on 31/10/15.
 */
public class StormOperator<T> extends OperatorBase implements WorkloadOperator<T> {
    private static final long serialVersionUID = 3305991729931748598L;
    protected TopologyBuilder topologyBuilder;
    protected String preComponentId;

    public StormOperator(TopologyBuilder builder, String previousComponent){
        this.topologyBuilder = builder;
        this.preComponentId = previousComponent;
    }

    @Override
    public <R> WorkloadOperator<R> map(MapFunction<T, R> fun, String componentId, int parallelism, boolean logThroughput) {
        MapBolt<T, R> bolt = new MapBolt<>(fun);
        if(logThroughput) {
            bolt.enableThroughput(componentId);
        }
        topologyBuilder.setBolt(componentId, bolt, parallelism).localOrShuffleGrouping(preComponentId);
        return new StormOperator<>(topologyBuilder, componentId);
    }

    @Override
    public <R> WorkloadOperator<R> map(MapFunction<T, R> fun, String componentId, int parallelism) {
        return map(fun, componentId, parallelism, false);
    }

    @Override
    public <K, V> PairWorkloadOperator<K, V> mapToPair(MapPairFunction<T, K, V> fun, String componentId, int parallelism, boolean logThroughput) {
        MapToPairBolt<T, K, V> bolt = new MapToPairBolt<>(fun);
        if(logThroughput) {
            bolt.enableThroughput(componentId);
        }
        topologyBuilder.setBolt(componentId, bolt, parallelism).localOrShuffleGrouping(preComponentId);
        return new StormPairOperator<>(topologyBuilder, componentId);
    }

    @Override
    public <K, V> PairWorkloadOperator<K, V> mapToPair(MapPairFunction<T, K, V> fun, String componentId, int parallelism) {
        return mapToPair(fun, componentId, parallelism, false);
    }

    @Override
    public WorkloadOperator<T> reduce(ReduceFunction<T> fun, String componentId, int parallelism, boolean logThroughput) {
        ReduceBolt<T> bolt = new ReduceBolt<>(fun);
        if(logThroughput) {
            bolt.enableThroughput(componentId);
        }
        topologyBuilder.setBolt(componentId, bolt, parallelism).localOrShuffleGrouping(preComponentId);
        return new StormOperator<>(topologyBuilder, componentId);
    }

    @Override
    public WorkloadOperator<T> reduce(ReduceFunction<T> fun, String componentId, int parallelism) {
        return reduce(fun, componentId, parallelism, false);
    }

    @Override
    public WorkloadOperator<T> filter(FilterFunction<T> fun, String componentId, int parallelism, boolean logThroughput) {
        FilterBolt<T> bolt = new FilterBolt<>(fun);
        if(logThroughput){
            bolt.enableThroughput(componentId);
        }
        topologyBuilder.setBolt(componentId, bolt, parallelism).localOrShuffleGrouping(preComponentId);
        return new StormOperator<>(topologyBuilder, componentId);
    }

    @Override
    public WorkloadOperator<T> filter(FilterFunction<T> fun, String componentId, int parallelism) {
        return  filter(fun, componentId, parallelism, false);
    }

    @Override
    public WorkloadOperator<T> iterative(MapFunction<T, T> mapFunction, FilterFunction<T> iterativeFunction,
                                         String componentId, int parallelism) {
        topologyBuilder.setBolt(componentId, new IteractiveBolt<>(mapFunction, iterativeFunction), parallelism)
                .localOrShuffleGrouping(preComponentId)
                .shuffleGrouping(componentId, IteractiveBolt.ITERATIVE_STREAM);
        return new StormOperator<>(topologyBuilder, componentId);
    }

    @Override
    public <R> WorkloadOperator<R> flatMap(FlatMapFunction<T, R> fun, String componentId, int parallelism, boolean logThroughput) {
        FlatMapBolt<T, R> bolt = new FlatMapBolt<>(fun);
        if(logThroughput){
            bolt.enableThroughput(componentId);
        }
        topologyBuilder.setBolt(componentId, bolt, parallelism).localOrShuffleGrouping(preComponentId);
        return new StormOperator<>(topologyBuilder, componentId);
    }

    @Override
    public <R> WorkloadOperator<R> flatMap(FlatMapFunction<T, R> fun, String componentId, int parallelism) {
        return flatMap(fun, componentId, parallelism, false);
    }

    @Override
    public <K, V> PairWorkloadOperator<K, V> flatMapToPair(FlatMapPairFunction<T, K, V> fun,
                                                           String componentId,
                                                           int parallelism,
                                                           boolean logThroughput) {
        FlatMapToPairBolt<T, K, V> bolt = new FlatMapToPairBolt<>(fun);
        if(logThroughput){
            bolt.enableThroughput(componentId);
        }
        topologyBuilder.setBolt(componentId, bolt, parallelism).localOrShuffleGrouping(preComponentId);
        return new StormPairOperator<>(topologyBuilder, componentId);
    }

    @Override
    public <K, V> PairWorkloadOperator<K, V> flatMapToPair(FlatMapPairFunction<T, K, V> fun, String componentId, int parallelism) {
        return flatMapToPair(fun, componentId, parallelism, false);
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

    @Override
    public void sink(int parallelism) {

    }
}
