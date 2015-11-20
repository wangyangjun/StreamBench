package fi.aalto.dmg.frame.bolts.discretized;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import fi.aalto.dmg.frame.bolts.BoltConstants;
import fi.aalto.dmg.frame.functions.ReduceFunction;
import org.apache.commons.collections.map.HashedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Discretized update bolt, accumulate key-value pairs
 * Created by jun on 11/13/15.
 * Tested
 */

public class DiscretizedPairUpdateBolt<K,V> extends DiscretizedBolt {
    private static final Logger logger = LoggerFactory.getLogger(DiscretizedPairReduceByKeyBolt.class);
    private static final long serialVersionUID = 3021160470219166181L;
    private ReduceFunction<V> fun;
    private Map<K,V> cumulatedDataMap;
    private Map<Integer, Map<K,V>> slideDataMap;

    public DiscretizedPairUpdateBolt(ReduceFunction<V> function, String preComponentId) {
        super(preComponentId);
        this.fun = function;
        cumulatedDataMap = new HashMap<K,V>();
        slideDataMap = new HashMap<>(BUFFER_SLIDES_NUM);
        for(int i=0; i<BUFFER_SLIDES_NUM; ++i)
            slideDataMap.put(i, new HashMap<K, V>());
    }

    @Override
    public void processTuple(Tuple tuple) {
        try{
            int slideId = tuple.getInteger(0);
            slideId = slideId%BUFFER_SLIDES_NUM;
            K key = (K) tuple.getValue(1);
            V value = (V) tuple.getValue(2);

            Map<K, V> slideMap = slideDataMap.get(slideId);
            if(null == slideMap){
                slideMap = new HashMap<>();
                slideMap.put(key, value);
                slideDataMap.put(slideId, slideMap);
            } else {
                V reducedValue = slideMap.get(key);
                if(null == reducedValue){
                    slideMap.put(key, value);
                } else {
                    slideMap.put(key, fun.reduce(reducedValue, value));
                }
            }
        } catch (Exception e) {
            logger.error(e.toString());
        }
    }

    @Override
    public void processSlide(BasicOutputCollector collector, int slideIndex) {
        try {
            // accumulate data
            Map<K, V> slideMap = slideDataMap.get(slideIndex);
            for (Map.Entry<K, V> entry : slideMap.entrySet()) {
                V accumulatedValue = cumulatedDataMap.get(entry.getKey());
                if (null == accumulatedValue) {
                    accumulatedValue = entry.getValue();
                } else {
                    accumulatedValue = fun.reduce(accumulatedValue, entry.getValue());
                }
                cumulatedDataMap.put(entry.getKey(), accumulatedValue);
                collector.emit(new Values(entry.getKey(), accumulatedValue));
            }
            slideMap.clear();
        } catch (Exception e){
            logger.error(e.toString());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        super.declareOutputFields(declarer);
        declarer.declareStream(Utils.DEFAULT_STREAM_ID,
                new Fields( BoltConstants.OutputKeyField, BoltConstants.OutputValueField));
    }
}
