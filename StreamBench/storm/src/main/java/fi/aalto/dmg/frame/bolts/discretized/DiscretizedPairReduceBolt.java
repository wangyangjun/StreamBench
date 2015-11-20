package fi.aalto.dmg.frame.bolts.discretized;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import fi.aalto.dmg.frame.bolts.BoltConstants;
import fi.aalto.dmg.frame.functions.ReduceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by jun on 11/13/15.
 */

public class DiscretizedPairReduceBolt<K,V> extends DiscretizedBolt {
    private static final Logger logger = LoggerFactory.getLogger(DiscretizedPairReduceByKeyBolt.class);
    private static final long serialVersionUID = -3392765120683897619L;
    private ReduceFunction<Tuple2<K, V>> fun;
    // each slide has corresponding reduced tuple
    private Map<Integer, Tuple2<K, V>> slideDataMap;

    public DiscretizedPairReduceBolt(ReduceFunction<Tuple2<K, V>> function, String preComponentId) {
        super(preComponentId);
        this.fun = function;
        slideDataMap = new HashMap<>(BUFFER_SLIDES_NUM);
    }

    @Override
    public void processTuple(Tuple tuple) {
        try{
            int slideId = tuple.getInteger(0);
            slideId = slideId%BUFFER_SLIDES_NUM;
            K key = (K) tuple.getValue(1);
            V value = (V) tuple.getValue(2);
            Tuple2<K,V> tuple2 = new Tuple2<>(key, value);

            Tuple2<K, V> slideReducedTuple = slideDataMap.get(slideId);
            if(null == slideReducedTuple){
                slideReducedTuple = tuple2;
            } else {
                slideReducedTuple = fun.reduce(slideReducedTuple, tuple2);
            }
            slideDataMap.put(slideId, slideReducedTuple);
        } catch (Exception e) {
            logger.error(e.toString());
        }
    }

    @Override
    public void processSlide(BasicOutputCollector collector, int slideIndex) {
        Tuple2<K, V> slideReducedTuple = slideDataMap.get(slideIndex);
        collector.emit(new Values(slideIndex, slideReducedTuple._1(), slideReducedTuple._2()));
        // clear data
        slideDataMap.put(slideIndex, null);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        super.declareOutputFields(declarer);
        declarer.declareStream(Utils.DEFAULT_STREAM_ID,
                new Fields(BoltConstants.OutputSlideIdField, BoltConstants.OutputKeyField, BoltConstants.OutputValueField));
    }
}
