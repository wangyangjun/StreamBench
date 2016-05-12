package fi.aalto.dmg.frame;

import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import fi.aalto.dmg.exceptions.UnsupportOperatorException;
import fi.aalto.dmg.frame.bolts.BoltConstants;
import fi.aalto.dmg.frame.bolts.PairReduceBolt;
import fi.aalto.dmg.frame.functions.ReduceFunction;

/**
 * Created by yangjun.wang on 01/11/15.
 */
public class StormGroupedOperator<K, V> extends GroupedWorkloadOperator<K, V> {

    private static final long serialVersionUID = 3901262136572311573L;
    protected TopologyBuilder topologyBuilder;
    protected String preComponentId;

    public StormGroupedOperator(TopologyBuilder builder, String previousComponent, int parallelism) {
        super(parallelism);
        this.topologyBuilder = builder;
        this.preComponentId = previousComponent;
    }

    @Override
    public PairWorkloadOperator<K, V> reduce(ReduceFunction<V> fun, String componentId, int parallelism) {

        topologyBuilder.setBolt(componentId,
                new PairReduceBolt<K, V>(fun),
                parallelism)
                .fieldsGrouping(preComponentId, new Fields(BoltConstants.OutputKeyField));
        return new StormPairOperator<>(topologyBuilder, componentId, parallelism);
    }

    @Override
    public void closeWith(OperatorBase stream, boolean broadcast) throws UnsupportOperatorException {
        throw new UnsupportOperatorException("not implemented yet");
    }

    @Override
    public void print() {

    }
}
