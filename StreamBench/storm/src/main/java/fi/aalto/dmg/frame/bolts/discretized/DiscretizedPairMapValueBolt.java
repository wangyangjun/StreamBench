package fi.aalto.dmg.frame.bolts.discretized;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import fi.aalto.dmg.frame.bolts.BoltConstants;
import fi.aalto.dmg.frame.functions.FilterFunction;
import fi.aalto.dmg.frame.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jun on 11/13/15.
 */

public class DiscretizedPairMapValueBolt<K, V, R> extends DiscretizedBolt {
    private static final Logger logger = LoggerFactory.getLogger(DiscretizedMapBolt.class);
    private static final long serialVersionUID = 8203174840485668437L;

    // each slide has a corresponding List<Tuple2<K,R>>
    private Map<Integer, List<Tuple2<K,R>>> slideDataMap;
    private MapFunction<Tuple2<K, V>, Tuple2<K, R>> fun;

    public DiscretizedPairMapValueBolt(MapFunction<Tuple2<K, V>, Tuple2<K, R>> function, String preComponentId) {
        super(preComponentId);
        this.fun = function;
        slideDataMap = new HashMap<>(BUFFER_SLIDES_NUM);
        for(int i=0; i<BUFFER_SLIDES_NUM; ++i){
            slideDataMap.put(i, new ArrayList<Tuple2<K,R>>());
        }
    }

    @Override
    public void processTuple(Tuple tuple) {
        try{
            int slideId = tuple.getInteger(0);
            slideId = slideId%BUFFER_SLIDES_NUM;
            K key = (K) tuple.getValue(1);
            V value = (V) tuple.getValue(2);
            List<Tuple2<K,R>> mapedList = slideDataMap.get(slideId);
            if(null == mapedList){
                mapedList = new ArrayList<>();
            }
            Tuple2<K,V> t = new Tuple2<>(key, value);
            mapedList.add(fun.map(t));
            slideDataMap.put(slideId, mapedList);
        } catch (Exception e) {
            logger.error(e.toString());
        }
    }

    @Override
    public void processSlide(BasicOutputCollector collector, int slideIndex) {
        List<Tuple2<K,R>> list = slideDataMap.get(slideIndex);
        for(Tuple2<K,R> t : list) {
            collector.emit(new Values(slideIndex, t._1(), t._2()));
        }
        // clear data
        list.clear();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        super.declareOutputFields(declarer);
        declarer.declareStream(Utils.DEFAULT_STREAM_ID,
                new Fields(BoltConstants.OutputSlideIdField, BoltConstants.OutputKeyField, BoltConstants.OutputValueField));
    }
}
