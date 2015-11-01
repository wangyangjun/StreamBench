package fi.aalto.dmg.frame;

import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import fi.aalto.dmg.frame.bolts.GroupedReduceBolt;
import fi.aalto.dmg.frame.functions.ReduceFunction;

/**
 * Created by yangjun.wang on 01/11/15.
 */
public class StormGroupedWorkloadOperator<K,V> implements GroupedWorkloadOperator<K,V>  {

    protected TopologyBuilder topologyBuilder;
    protected String preComponentId;

    public StormGroupedWorkloadOperator(TopologyBuilder builder, String previousComponent){
        this.topologyBuilder = builder;
        this.preComponentId = previousComponent;
    }

    @Override
    public PairedWorkloadOperator<K, V> reduce(ReduceFunction<V> fun, String componentId) {

        topologyBuilder.setBolt(componentId, new GroupedReduceBolt<K,V>(fun)).fieldsGrouping(preComponentId, new Fields("key"));
        return new StormPairedWordloadOperator<>(topologyBuilder, componentId);
    }
}
