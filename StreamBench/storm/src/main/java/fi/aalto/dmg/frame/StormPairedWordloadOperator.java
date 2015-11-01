package fi.aalto.dmg.frame;

import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import fi.aalto.dmg.frame.bolts.GroupedReduceBolt;
import fi.aalto.dmg.frame.bolts.PairPrintBolt;
import fi.aalto.dmg.frame.bolts.PrintBolt;
import fi.aalto.dmg.frame.functions.ReduceFunction;
import fi.aalto.dmg.frame.functions.UpdateStateFunction;
import scala.Tuple2;

/**
 * Created by yangjun.wang on 31/10/15.
 */
public class StormPairedWordloadOperator<K, V> extends StormWorkloadOperator<Tuple2<K,V>> implements PairedWorkloadOperator<K,V> {

    public StormPairedWordloadOperator(TopologyBuilder builder, String previousComponent) {
        super(builder, previousComponent);
    }

    @Override
    public GroupedWorkloadOperator<K, V> groupByKey() {
        return new StormGroupedWorkloadOperator<>(topologyBuilder, this.preComponentId);
    }

    // Set bolt with fieldsGrouping
    @Override
    public PairedWorkloadOperator<K, V> reduceByKey(ReduceFunction<V> fun, String componentId) {
        topologyBuilder.setBolt(componentId, new GroupedReduceBolt<K,V>(fun)).fieldsGrouping(preComponentId, new Fields("key"));
        return new StormPairedWordloadOperator<>(topologyBuilder, componentId);
    }

    // Set bolt with fieldsGrouping
    @Override
    public PairedWorkloadOperator<K, V> updateStateByKey(UpdateStateFunction<V> fun, String componentId) {
        return this;
    }

    @Override
    public void print() {
        topologyBuilder.setBolt("print", new PairPrintBolt<>()).localOrShuffleGrouping(preComponentId);
    }

}
