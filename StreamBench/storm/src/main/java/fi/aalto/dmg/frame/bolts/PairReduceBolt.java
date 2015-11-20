package fi.aalto.dmg.frame.bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import fi.aalto.dmg.frame.functions.ReduceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yangjun.wang on 01/11/15.
 */
public class PairReduceBolt<K,V> extends BaseBasicBolt {

    private static final Logger logger = LoggerFactory.getLogger(PairReduceBolt.class);
    private static final long serialVersionUID = 3751131798984227211L;
    private Map<K, V> map;

    ReduceFunction<V> fun;

    public PairReduceBolt(ReduceFunction<V> function){
        this.fun = function;
        map = new HashMap<>();
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        Object k = input.getValue(0);
        Object v = input.getValue(1);
        V currentValue = map.get(k);
        try {
            K key = (K)k;
            if(null != currentValue){
                currentValue = this.fun.reduce((V) v, currentValue);
            } else {
                currentValue = (V)v;
            }
            map.put(key, currentValue);
            collector.emit(new Values(key, currentValue));
        } catch (ClassCastException e){
            logger.error("Cast tuple[0] failed");
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Reduce error");
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(BoltConstants.OutputKeyField, BoltConstants.OutputValueField));
    }
}
