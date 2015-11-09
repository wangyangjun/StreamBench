package fi.aalto.dmg.frame.bolts;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import fi.aalto.dmg.exceptions.DurationException;
import fi.aalto.dmg.frame.functions.ReduceFunction;
import fi.aalto.dmg.util.TimeDurations;
import fi.aalto.dmg.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Online calculate(default)/ Cumulative data
 * Created by jun on 11/9/15.
 */
public class WindowPairReduceBolt<K,V> extends BaseBasicBolt {

    private static final Logger logger = LoggerFactory.getLogger(WindowPairReduceBolt.class);
    private List<Map<K, V>> maps;
    private long WINDOW_TICK_FREQUENCY_SECONDES;
    private long SLIDE_TICK_FREQUENCY_SECONDES;
    private int slides;
    private int current_silde;

    ReduceFunction<V> fun;

    public WindowPairReduceBolt(ReduceFunction<V> function, TimeDurations windowDuration, TimeDurations slideDuration) throws DurationException {
        this.fun = function;
        WINDOW_TICK_FREQUENCY_SECONDES = Utils.getSeconds(windowDuration);
        SLIDE_TICK_FREQUENCY_SECONDES = Utils.getSeconds(slideDuration);
        if(WINDOW_TICK_FREQUENCY_SECONDES%SLIDE_TICK_FREQUENCY_SECONDES != 0){
            throw new DurationException("Window duration should be multi times of slide duration.");
        }
        slides = (int) (WINDOW_TICK_FREQUENCY_SECONDES/SLIDE_TICK_FREQUENCY_SECONDES);
        current_silde = 0;
        maps = new ArrayList<>(slides);
        for(int i=0; i<slides; ++i) {
            maps.add(new HashMap<K, V>());
        }
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        try {
            if(isTickTuple(tuple)){
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
                    collector.emit(new Values(entry.getKey(), entry.getValue()));
                }
                current_silde = (current_silde+1)%slides;
                maps.get(current_silde).clear();
            } else {
                Map<K, V> map = maps.get(current_silde);
                K key = (K)tuple.getValue(0);
                V value = (V)tuple.getValue(1);
                V reducedValue = map.get(key);
                if (null == reducedValue)
                    map.put(key, value);
                else{
                    map.put(key, fun.reduce(reducedValue, value));
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(BoltConstants.OutputKeyField, BoltConstants.OutputValueField));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, SLIDE_TICK_FREQUENCY_SECONDES);
        return conf;
    }

    private static boolean isTickTuple(Tuple tuple){
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }
}
