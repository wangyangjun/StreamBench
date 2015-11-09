package fi.aalto.dmg.frame.bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.common.base.Optional;
import fi.aalto.dmg.frame.functions.ReduceFunction;
import fi.aalto.dmg.frame.functions.UpdateStateFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jun on 11/9/15.
 */
public class UpdateStateBolt<K,V> extends BaseBasicBolt {

    private static final Logger logger = LoggerFactory.getLogger(PairReduceBolt.class);
    private Map<K, V> map;

    UpdateStateFunction<V> fun;

    public UpdateStateBolt(UpdateStateFunction<V> function){
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
                List<V> list = new ArrayList<>();
                list.add((V)v);
                currentValue = this.fun.update(list, Optional.of(currentValue)).get();
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