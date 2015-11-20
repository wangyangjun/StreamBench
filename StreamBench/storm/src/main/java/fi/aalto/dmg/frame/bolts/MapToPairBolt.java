package fi.aalto.dmg.frame.bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import fi.aalto.dmg.frame.functions.MapPairFunction;
import scala.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yangjun.wang on 31/10/15.
 */
public class MapToPairBolt<T, K, V> extends BaseBasicBolt {
    private static final Logger logger = LoggerFactory.getLogger(MapToPairBolt.class);
    private static final long serialVersionUID = 713275144540880633L;

    MapPairFunction<T, K, V> fun;

    public MapToPairBolt(MapPairFunction<T, K, V> function){
        this.fun = function;
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        Object o = input.getValue(0);
        try {
            Tuple2<K,V> result = this.fun.mapPair((T) o);
            collector.emit(new Values(result._1(), result._2()));
        } catch (ClassCastException e){
            logger.error("Cast tuple[0] failed");
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(BoltConstants.OutputKeyField, BoltConstants.OutputValueField));
    }
}
