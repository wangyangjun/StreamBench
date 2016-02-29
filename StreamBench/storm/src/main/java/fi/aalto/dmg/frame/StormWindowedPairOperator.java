package fi.aalto.dmg.frame;

import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import fi.aalto.dmg.exceptions.DurationException;
import fi.aalto.dmg.exceptions.UnsupportOperatorException;
import fi.aalto.dmg.frame.bolts.*;
import fi.aalto.dmg.frame.bolts.windowed.*;
import fi.aalto.dmg.frame.functions.*;
import fi.aalto.dmg.util.TimeDurations;
import org.apache.log4j.Logger;
import scala.Tuple2;


/**
 * Created by jun on 11/9/15.
 */
public class StormWindowedPairOperator<K,V> extends WindowedPairWorkloadOperator<K,V>{

    private static final long serialVersionUID = -2953749652496717735L;
    private TopologyBuilder topologyBuilder;
    private String preComponentId;
    private TimeDurations windowDuration;
    private TimeDurations slideDuration;

    /**
     * @param builder
     * @param previousComponent
     * @param windowDuration
     * @param slideDuration
     */
    public StormWindowedPairOperator(TopologyBuilder builder, String previousComponent, TimeDurations windowDuration,
                                     TimeDurations slideDuration, int parallelism) {
        super(parallelism);
        this.topologyBuilder = builder;
        this.preComponentId = previousComponent;
        this.windowDuration = windowDuration;
        this.slideDuration = slideDuration;
    }


    @Override
    public PairWorkloadOperator<K, V> reduceByKey(ReduceFunction<V> fun, String componentId, boolean logThroughput) {
        try {
            WindowPairReduceByKeyBolt<K, V> bolt = new WindowPairReduceByKeyBolt<>(fun, windowDuration, slideDuration);
            if(logThroughput) {
                bolt.enableThroughput(componentId);
            }
            topologyBuilder.setBolt(componentId, bolt, parallelism)
                    .fieldsGrouping(preComponentId, new Fields(BoltConstants.OutputKeyField));
        } catch (DurationException e) {
            e.printStackTrace();
        }
        return new StormPairOperator<>(topologyBuilder, componentId, parallelism);
    }

    @Override
    public PairWorkloadOperator<K, V> reduceByKey(ReduceFunction<V> fun, String componentId) {
        return reduceByKey(fun, componentId, false);
    }

    @Override
    public PairWorkloadOperator<K, V> updateStateByKey(ReduceFunction<V> fun, String componentId, boolean logThroughput) {
        return null;
    }

    /**
     * It seems that no one will call this function
     * @param fun
     * @param componentId
     * @return
     */
    @Override
    public PairWorkloadOperator<K, V> updateStateByKey(ReduceFunction<V> fun, String componentId) {
//        try {
//            topologyBuilder.setBolt(componentId, new UpdateStateBolt<>(fun, windowDuration, slideDuration))
//                    .fieldsGrouping(preComponentId, new Fields(BoltConstants.OutputKeyField));
//        } catch (DurationException e) {
//            e.printStackTrace();
//        }
//        return new StormPairOperator<>(topologyBuilder, componentId);
        return null;
    }

    @Override
    public <R> PairWorkloadOperator<K, R> mapPartition(MapPartitionFunction<Tuple2<K, V>, Tuple2<K, R>> fun,
                                                       String componentId, boolean logThroughput) {
        try {
            WindowPairMapPartitionBolt<K, V, R> bolt = new WindowPairMapPartitionBolt<>(fun, windowDuration, slideDuration);
            if(logThroughput) {
                bolt.enableThroughput(componentId);
            }
            topologyBuilder.setBolt(componentId, bolt, parallelism)
                    .fieldsGrouping(preComponentId, new Fields(BoltConstants.OutputKeyField));
        } catch (DurationException e) {
            e.printStackTrace();
        }
        return new StormPairOperator<>(topologyBuilder, componentId, parallelism);
    }

    @Override
    public <R> PairWorkloadOperator<K, R> mapPartition(MapPartitionFunction<Tuple2<K, V>, Tuple2<K, R>> fun,
                                                       String componentId) {
        return mapPartition(fun, componentId, false);
    }

    @Override
    public <R> PairWorkloadOperator<K, R> mapValue(MapFunction<Tuple2<K, V>, Tuple2<K, R>> fun,
                                                   String componentId, boolean logThroughput) {
        try {
            WindowMapValueBolt<K, V, R> bolt = new WindowMapValueBolt<>(fun, windowDuration, slideDuration);
            if(logThroughput) {
                bolt.enableThroughput(componentId);
            }
            topologyBuilder.setBolt(componentId, bolt, parallelism)
                    .fieldsGrouping(preComponentId, new Fields(BoltConstants.OutputKeyField));
        } catch (DurationException e) {
            e.printStackTrace();
        }
        return new StormPairOperator<>(topologyBuilder, componentId, parallelism);
    }

    @Override
    public <R> PairWorkloadOperator<K, R> mapValue(MapFunction<Tuple2<K, V>, Tuple2<K, R>> fun, String componentId) {
        return mapValue(fun, componentId, false);
    }

    @Override
    public PairWorkloadOperator<K, V> filter(FilterFunction<Tuple2<K, V>> fun,
                                             String componentId, boolean logThroughput) {
        try {
            WindowPairFilterBolt<K, V> bolt = new WindowPairFilterBolt<>(fun, windowDuration, slideDuration);
            if(logThroughput) {
                bolt.enableThroughput(componentId);
            }
            topologyBuilder.setBolt(componentId, bolt, parallelism)
                    .fieldsGrouping(preComponentId, new Fields(BoltConstants.OutputKeyField));
        } catch (DurationException e) {
            e.printStackTrace();
        }
        return new StormPairOperator<>(topologyBuilder, componentId, parallelism);
    }

    @Override
    public PairWorkloadOperator<K, V> filter(FilterFunction<Tuple2<K, V>> fun, String componentId) {
        return filter(fun, componentId, false);
    }

    @Override
    public PairWorkloadOperator<K, V> reduce(ReduceFunction<Tuple2<K, V>> fun, String componentId,
                                             boolean logThroughput) {
        try {
            WindowPairReduceBolt<K, V> bolt = new WindowPairReduceBolt<>(fun, windowDuration, slideDuration);
            if(logThroughput) {
                bolt.enableThroughput(componentId);
            }
            topologyBuilder.setBolt(componentId, bolt, parallelism)
                    .fieldsGrouping(preComponentId, new Fields(BoltConstants.OutputKeyField));
        } catch (DurationException e) {
            e.printStackTrace();
        }
        return new StormPairOperator<>(topologyBuilder, componentId, parallelism);
    }

    @Override
    public PairWorkloadOperator<K, V> reduce(ReduceFunction<Tuple2<K, V>> fun, String componentId) {
        return  reduce(fun, componentId, false);
    }

    @Override
    public void closeWith(OperatorBase stream, boolean broadcast) throws UnsupportOperatorException {
        throw new UnsupportOperatorException("not implemented yet");
    }

    @Override
    public void print() {
        topologyBuilder.setBolt("print", new PairPrintBolt<>(true)).localOrShuffleGrouping(preComponentId);
    }
}
