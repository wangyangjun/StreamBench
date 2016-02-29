package fi.aalto.dmg.frame;

import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import fi.aalto.dmg.exceptions.UnsupportOperatorException;
import fi.aalto.dmg.frame.bolts.*;
import fi.aalto.dmg.frame.functions.*;
import fi.aalto.dmg.util.TimeDurations;

import java.util.List;

/**
 * Created by yangjun.wang on 31/10/15.
 */
public class StormOperator<T> extends  WorkloadOperator<T> {
    private static final long serialVersionUID = 3305991729931748598L;
    protected TopologyBuilder topologyBuilder;
    protected String preComponentId;
    private BoltDeclarer boltDeclarer;

    public StormOperator(TopologyBuilder builder, String previousComponent, int parallelism){
        super(parallelism);
        this.topologyBuilder = builder;
        this.preComponentId = previousComponent;
    }

    @Override
    public <R> WorkloadOperator<R> map(MapFunction<T, R> fun, String componentId, boolean logThroughput) {
        MapBolt<T, R> bolt = new MapBolt<>(fun);
        if(logThroughput) {
            bolt.enableThroughput(componentId);
        }
        boltDeclarer = topologyBuilder.setBolt(componentId, bolt, parallelism).localOrShuffleGrouping(preComponentId);
        return new StormOperator<>(topologyBuilder, componentId, parallelism);
    }

    @Override
    public <R> WorkloadOperator<R> map(MapFunction<T, R> fun, String componentId) {
        return map(fun, componentId, false);
    }

    @Override
    public <R> WorkloadOperator<R> map(MapWithInitListFunction<T, R> fun, List<T> initList, String componentId) {
        MapWithInitListBolt<T, R> bolt = new MapWithInitListBolt<>(fun, initList);
        boltDeclarer = topologyBuilder.setBolt(componentId, bolt, parallelism).localOrShuffleGrouping(preComponentId);
        return new StormOperator<>(topologyBuilder, componentId, parallelism);
    }

    @Override
    public <R> WorkloadOperator<R> map(MapWithInitListFunction<T, R> fun, List<T> initList, String componentId, Class<R> outputClass) throws UnsupportOperatorException {
        return map(fun, initList, componentId);
    }

    @Override
    public <K, V> PairWorkloadOperator<K, V> mapToPair(MapPairFunction<T, K, V> fun, String componentId, boolean logThroughput) {
        MapToPairBolt<T, K, V> bolt = new MapToPairBolt<>(fun);
        if(logThroughput) {
            bolt.enableThroughput(componentId);
        }
        boltDeclarer = topologyBuilder.setBolt(componentId, bolt, parallelism).localOrShuffleGrouping(preComponentId);
        return new StormPairOperator<>(topologyBuilder, componentId, parallelism);
    }

    @Override
    public <K, V> PairWorkloadOperator<K, V> mapToPair(MapPairFunction<T, K, V> fun, String componentId) {
        return mapToPair(fun, componentId, false);
    }

    @Override
    public WorkloadOperator<T> reduce(ReduceFunction<T> fun, String componentId, boolean logThroughput) {
        ReduceBolt<T> bolt = new ReduceBolt<>(fun);
        if(logThroughput) {
            bolt.enableThroughput(componentId);
        }
        boltDeclarer = topologyBuilder.setBolt(componentId, bolt, parallelism).localOrShuffleGrouping(preComponentId);
        return new StormOperator<>(topologyBuilder, componentId, parallelism);
    }

    @Override
    public WorkloadOperator<T> reduce(ReduceFunction<T> fun, String componentId) {
        return reduce(fun, componentId, false);
    }

    @Override
    public WorkloadOperator<T> filter(FilterFunction<T> fun, String componentId, boolean logThroughput) {
        FilterBolt<T> bolt = new FilterBolt<>(fun);
        if(logThroughput){
            bolt.enableThroughput(componentId);
        }
        boltDeclarer = topologyBuilder.setBolt(componentId, bolt, parallelism).localOrShuffleGrouping(preComponentId);
        return new StormOperator<>(topologyBuilder, componentId, parallelism);
    }

    @Override
    public WorkloadOperator<T> filter(FilterFunction<T> fun, String componentId) {
        return  filter(fun, componentId, false);
    }

    @Override
    public <R> WorkloadOperator<R> flatMap(FlatMapFunction<T, R> fun, String componentId, boolean logThroughput) {
        FlatMapBolt<T, R> bolt = new FlatMapBolt<>(fun);
        if(logThroughput){
            bolt.enableThroughput(componentId);
        }
        boltDeclarer = topologyBuilder.setBolt(componentId, bolt, parallelism).localOrShuffleGrouping(preComponentId);
        return new StormOperator<>(topologyBuilder, componentId, parallelism);
    }

    @Override
    public <R> WorkloadOperator<R> flatMap(FlatMapFunction<T, R> fun, String componentId) {
        return flatMap(fun, componentId, false);
    }

    @Override
    public <K, V> PairWorkloadOperator<K, V> flatMapToPair(FlatMapPairFunction<T, K, V> fun,
                                                           String componentId,
                                                           boolean logThroughput) {
        FlatMapToPairBolt<T, K, V> bolt = new FlatMapToPairBolt<>(fun);
        if(logThroughput){
            bolt.enableThroughput(componentId);
        }
        boltDeclarer = topologyBuilder.setBolt(componentId, bolt, parallelism).localOrShuffleGrouping(preComponentId);
        return new StormPairOperator<>(topologyBuilder, componentId, parallelism);
    }

    @Override
    public <K, V> PairWorkloadOperator<K, V> flatMapToPair(FlatMapPairFunction<T, K, V> fun, String componentId) {
        return flatMapToPair(fun, componentId, false);
    }

    @Override
    public WindowedWorkloadOperator<T> window(TimeDurations windowDuration) {
        return window(windowDuration, windowDuration);
    }

    @Override
    public WindowedWorkloadOperator<T> window(TimeDurations windowDuration, TimeDurations slideDuration) {
        return new StormWindowedOperator<T>(topologyBuilder, preComponentId, windowDuration, slideDuration, parallelism);
    }

    @Override
    public void closeWith(OperatorBase stream, boolean broadcast) throws UnsupportOperatorException {
        if(null == boltDeclarer) {
            throw new UnsupportOperatorException("boltDeclarer could not be null");
        } else if( !stream.getClass().equals(this.getClass())) {
            throw new UnsupportOperatorException("The close stream should be the same type of the origin stream");
        } else if(!this.iterative_enabled) {
            throw new UnsupportOperatorException("Iterative is not enabled.");
        } else {
            StormOperator<T> stream_close = (StormOperator<T>) stream;
            if(broadcast) {
                boltDeclarer.allGrouping(stream_close.preComponentId);
            } else {
                boltDeclarer.shuffleGrouping(stream_close.preComponentId);
            }
        }
        this.isIterative_closed = true;
    }

    @Override
    public void print() {
        boltDeclarer = topologyBuilder.setBolt("print"+preComponentId, new PrintBolt<T>()).localOrShuffleGrouping(preComponentId);
    }

    @Override
    public void sink() {

    }
}
