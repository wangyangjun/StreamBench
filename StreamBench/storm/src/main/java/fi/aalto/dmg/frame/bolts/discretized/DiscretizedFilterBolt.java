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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jun on 12/11/15.
 */
public class DiscretizedFilterBolt<T> extends DiscretizedBolt {
    private static final Logger logger = LoggerFactory.getLogger(DiscretizedMapBolt.class);

    private Map<Integer, List<T>> slideDataMap;
    private FilterFunction<T> fun;

    public DiscretizedFilterBolt(FilterFunction<T> function, String preComponentId) {
        super(preComponentId);
        this.fun = function;
        slideDataMap = new HashMap<>(BUFFER_SLIDES_NUM);
        for(int i=0; i<BUFFER_SLIDES_NUM; ++i){
            slideDataMap.put(i, new ArrayList<T>());
        }
    }

    @Override
    public void processTuple(Tuple tuple) {
        try{
            int slideId = tuple.getInteger(0);
            slideId = slideId%BUFFER_SLIDES_NUM;
            T t = (T) tuple.getValue(1);
            List<T> filterList = slideDataMap.get(slideId);
            if(null == filterList){
                filterList = new ArrayList<>();

            }
            if(fun.filter(t)){
                filterList.add(t);
            }
            slideDataMap.put(slideId, filterList);
        } catch (Exception e) {
            logger.error(e.toString());
        }
    }

    @Override
    public void processSlide(BasicOutputCollector collector, int slideIndex) {
        List<T> list = slideDataMap.get(slideIndex);
        for(T t : list) {
            collector.emit(new Values(slideIndex, t));
        }
        // clear data
        list.clear();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        super.declareOutputFields(declarer);
        declarer.declareStream(Utils.DEFAULT_STREAM_ID,
                new Fields(BoltConstants.OutputSlideIdField, BoltConstants.OutputValueField));
    }
}
