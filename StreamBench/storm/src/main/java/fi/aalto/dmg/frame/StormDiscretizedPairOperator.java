//package fi.aalto.dmg.frame;
//
//import backtype.storm.topology.TopologyBuilder;
//import backtype.storm.tuple.Fields;
//import fi.aalto.dmg.frame.bolts.BoltConstants;
//import fi.aalto.dmg.frame.bolts.PairPrintBolt;
//import fi.aalto.dmg.frame.bolts.discretized.*;
//import fi.aalto.dmg.frame.functions.*;
//import scala.Tuple2;
//
///**
// * Created by jun on 11/12/15.
// */
//public class StormDiscretizedPairOperator<K,V> implements WindowedPairWorkloadOperator<K,V> {
//
//    private TopologyBuilder topologyBuilder;
//    private String preComponentId;
//
//    public StormDiscretizedPairOperator(TopologyBuilder builder, String previousComponent) {
//        this.topologyBuilder = builder;
//        this.preComponentId = previousComponent;
//    }
//
//    @Override
//    public WindowedPairWorkloadOperator<K, V> reduceByKey(ReduceFunction<V> fun, String componentId) {
//        topologyBuilder.setBolt(componentId, new DiscretizedPairReduceByKeyBolt<>(fun, preComponentId))
//                .fieldsGrouping(preComponentId, new Fields(BoltConstants.OutputKeyField))
//                .allGrouping(preComponentId, BoltConstants.TICK_STREAM_ID);
//        return new StormDiscretizedPairOperator<>(topologyBuilder, componentId);
//    }
//
//    @Override
//    public PairWorkloadOperator<K, V> updateStateByKey(ReduceFunction<V> fun, String componentId) {
//        topologyBuilder.setBolt(componentId, new DiscretizedPairUpdateBolt<>(fun, preComponentId))
//                .fieldsGrouping(preComponentId, new Fields(BoltConstants.OutputKeyField))
//                .allGrouping(preComponentId, BoltConstants.TICK_STREAM_ID);
//        return new StormPairOperator<>(topologyBuilder, componentId);
//    }
//
//    @Override
//    public <R> WindowedPairWorkloadOperator<K, R> mapPartition(MapPartitionFunction<Tuple2<K, V>, Tuple2<K, R>> fun, String componentId) {
//        topologyBuilder.setBolt(componentId, new DiscretizedPairMapPartitionBolt<>(fun, preComponentId))
//                .fieldsGrouping(preComponentId, new Fields(BoltConstants.OutputKeyField))
//                .allGrouping(preComponentId, BoltConstants.TICK_STREAM_ID);
//        return new StormDiscretizedPairOperator<>(topologyBuilder, componentId);
//    }
//
//    @Override
//    public <R> WindowedPairWorkloadOperator<K, R> mapValue(MapFunction<Tuple2<K, V>, Tuple2<K, R>> fun, String componentId) {
//        topologyBuilder.setBolt(componentId, new DiscretizedPairMapValueBolt<>(fun, preComponentId))
//                .fieldsGrouping(preComponentId, new Fields(BoltConstants.OutputKeyField))
//                .allGrouping(preComponentId, BoltConstants.TICK_STREAM_ID);
//        return new StormDiscretizedPairOperator<>(topologyBuilder, componentId);
//    }
//
//    @Override
//    public WindowedPairWorkloadOperator<K, V> filter(FilterFunction<Tuple2<K, V>> fun, String componentId) {
//        topologyBuilder.setBolt(componentId, new DiscretizedPairFilterBolt<>(fun, preComponentId))
//                .fieldsGrouping(preComponentId, new Fields(BoltConstants.OutputKeyField))
//                .allGrouping(preComponentId, BoltConstants.TICK_STREAM_ID);
//        return new StormDiscretizedPairOperator<>(topologyBuilder, componentId);
//    }
//
//    @Override
//    public WindowedPairWorkloadOperator<K, V> reduce(ReduceFunction<Tuple2<K, V>> fun, String componentId) {
//        topologyBuilder.setBolt(componentId, new DiscretizedPairReduceBolt<>(fun, preComponentId))
//                .fieldsGrouping(preComponentId, new Fields(BoltConstants.OutputKeyField))
//                .allGrouping(preComponentId, BoltConstants.TICK_STREAM_ID);
//        return new StormDiscretizedPairOperator<>(topologyBuilder, componentId);
//    }
//
//    @Override
//    public void print() {
//        topologyBuilder.setBolt("print", new PairPrintBolt<>(true)).localOrShuffleGrouping(preComponentId);
//    }
//}
