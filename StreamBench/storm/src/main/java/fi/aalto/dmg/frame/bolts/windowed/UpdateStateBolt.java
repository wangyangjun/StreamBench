package fi.aalto.dmg.frame.bolts.windowed;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import fi.aalto.dmg.exceptions.DurationException;
import fi.aalto.dmg.frame.bolts.BoltConstants;
import fi.aalto.dmg.frame.bolts.PairReduceBolt;
import fi.aalto.dmg.frame.functions.ReduceFunction;
import fi.aalto.dmg.util.TimeDurations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jun on 11/9/15.
 */
public class UpdateStateBolt<K,V> extends WindowedBolt  {

    private static final Logger logger = LoggerFactory.getLogger(PairReduceBolt.class);
    private static final long serialVersionUID = -5859171958528281638L;
    private List<Map<K, V>> maps;

    ReduceFunction<V> fun;

    public UpdateStateBolt(ReduceFunction<V> function, TimeDurations windowDuration, TimeDurations slideDuration) throws DurationException {
        super(windowDuration, slideDuration);
        this.fun = function;
        maps = new ArrayList<>(WINDOW_SIZE);
        for(int i=0; i<WINDOW_SIZE; ++i) {
            maps.add(new HashMap<K, V>());
        }
    }

    @Override
    public void processTuple(Tuple tuple) {
        try {
            Map<K, V> map = maps.get(slideInWindow);
            K key = (K)tuple.getValue(0);
            V value = (V)tuple.getValue(1);
            V reducedValue = map.get(key);
            if (null == reducedValue)
                map.put(key, value);
            else{
                map.put(key, fun.reduce(reducedValue, value));
            }
        } catch (Exception e) {
            logger.error(e.toString());
        }
    }

    @Override
    public void processSlide(BasicOutputCollector collector) {
        try{
            Map<K, V> reduceMap = new HashMap<>();
            for(Map<K,V> map : maps){
                for(Map.Entry<K,V> entry : map.entrySet()){
                    V reducedValue = reduceMap.get(entry.getKey());
                    if(null == reducedValue){
                        reduceMap.put(entry.getKey(), entry.getValue());
                    } else {
                        reduceMap.put(entry.getKey(), fun.reduce(reducedValue, entry.getValue()));
                    }
                }
            }
            for(Map.Entry<K, V> entry: reduceMap.entrySet()){
                collector.emit(new Values(slideIndexInBuffer, entry.getKey(), entry.getValue()));
            }
            // there is no need to clear data
        } catch (Exception e) {
            logger.error(e.toString());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        super.declareOutputFields(declarer);
        declarer.declare(new Fields(BoltConstants.OutputSlideIdField, BoltConstants.OutputKeyField, BoltConstants.OutputValueField));
    }
}