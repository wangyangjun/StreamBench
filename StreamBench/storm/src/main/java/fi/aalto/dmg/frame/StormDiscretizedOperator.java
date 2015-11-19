//package fi.aalto.dmg.frame;
//
//import backtype.storm.topology.TopologyBuilder;
//import fi.aalto.dmg.frame.bolts.BoltConstants;
//import fi.aalto.dmg.frame.bolts.PrintBolt;
//import fi.aalto.dmg.frame.bolts.discretized.*;
//import fi.aalto.dmg.frame.functions.*;
//
///**
// * Storm discretized stream operator
// * it is generated after window operator
// * Created by jun on 11/12/15.
// */
//public class StormDiscretizedOperator<T> implements WindowedWorkloadOperator<T> {
//
//    protected TopologyBuilder topologyBuilder;
//    protected String preComponentId;
//
//    public StormDiscretizedOperator(TopologyBuilder builder, String previousComponent) {
//        this.topologyBuilder = builder;
//        this.preComponentId = previousComponent;
//    }
//
//
//    @Override
//    public <R> WindowedWorkloadOperator<R> mapPartition(MapPartitionFunction<T, R> fun, String componentId) {
//        topologyBuilder.setBolt(componentId, new DiscretizedMapPartitionBolt<>(fun, preComponentId))
//                .localOrShuffleGrouping(preComponentId)
//                .allGrouping(preComponentId, BoltConstants.TICK_STREAM_ID);
//        return new StormDiscretizedOperator<>(topologyBuilder, componentId);
//    }
//
//    @Override
//    public <R> WindowedWorkloadOperator<R> map(MapFunction<T, R> fun, String componentId) {
//        topologyBuilder.setBolt(componentId, new DiscretizedMapBolt<>(fun, preComponentId))
//                .localOrShuffleGrouping(preComponentId)
//                .allGrouping(preComponentId, BoltConstants.TICK_STREAM_ID);
//        return new StormDiscretizedOperator<>(topologyBuilder, componentId);
//    }
//
//    @Override
//    public WindowedWorkloadOperator<T> filter(FilterFunction<T> fun, String componentId) {
//        topologyBuilder.setBolt(componentId, new DiscretizedFilterBolt<>(fun, preComponentId))
//                .localOrShuffleGrouping(preComponentId)
//                .allGrouping(preComponentId, BoltConstants.TICK_STREAM_ID);
//        return new StormDiscretizedOperator<>(topologyBuilder, componentId);
//    }
//
//    @Override
//    public WindowedWorkloadOperator<T> reduce(ReduceFunction<T> fun, String componentId) {
//        topologyBuilder.setBolt(componentId, new DiscretizedReduceBolt<>(fun, preComponentId))
//                .localOrShuffleGrouping(preComponentId)
//                .allGrouping(preComponentId, BoltConstants.TICK_STREAM_ID);
//        return new StormDiscretizedOperator<>(topologyBuilder, componentId);
//    }
//
//    @Override
//    public <K, V> WindowedPairWorkloadOperator<K, V> mapToPair(MapPairFunction<T, K, V> fun, String componentId) {
//        topologyBuilder.setBolt(componentId, new DiscretizedMapToPairBolt<>(fun, preComponentId))
//                .localOrShuffleGrouping(preComponentId)
//                .allGrouping(preComponentId, BoltConstants.TICK_STREAM_ID);
//        return new StormDiscretizedPairOperator<>(topologyBuilder, componentId);
//    }
//
//    @Override
//    public void print() {
//        topologyBuilder.setBolt("print", new PrintBolt<>(true)).localOrShuffleGrouping(preComponentId);
//    }
//}
