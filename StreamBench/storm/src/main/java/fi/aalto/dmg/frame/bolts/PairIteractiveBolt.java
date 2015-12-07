package fi.aalto.dmg.frame.bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import fi.aalto.dmg.frame.functions.FilterFunction;
import fi.aalto.dmg.frame.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 * Created by jun on 02/12/15.
 */
public class PairIteractiveBolt<K,V> extends BaseBasicBolt {

    private static final Logger logger = LoggerFactory.getLogger(FilterBolt.class);
    private static final long serialVersionUID = 2401869900116259991L;
    public static String ITERATIVE_STREAM = "IterativeStream";

    FilterFunction<Tuple2<K,V>> filterFunction;
    MapFunction<V, V> mapFunction;

    public PairIteractiveBolt(MapFunction<V, V> mapFunction, FilterFunction<Tuple2<K,V>> filterFunction) {
        this.mapFunction = mapFunction;
        this.filterFunction = filterFunction;
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        Object k = input.getValue(0);
        Object v = input.getValue(1);
        try {
            V result = this.mapFunction.map((V) v);
            if (this.filterFunction.filter(new Tuple2<K, V>((K)k, result))) {
                collector.emit(ITERATIVE_STREAM, new Values(k, result));

            } else {
                collector.emit(new Values(k, result));
            }
        } catch (ClassCastException e) {
            logger.error("Cast tuple[0] failed");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(ITERATIVE_STREAM, new Fields(BoltConstants.OutputKeyField, BoltConstants.OutputValueField));
        declarer.declare(new Fields(BoltConstants.OutputKeyField, BoltConstants.OutputValueField));
    }

}
