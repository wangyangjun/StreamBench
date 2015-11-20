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

import java.util.HashMap;
import java.util.Map;

/**
 * Created by jun on 12/11/15.
 */
public class DiscretizedReduceBolt<T> extends DiscretizedBolt {

    private static final Logger logger = LoggerFactory.getLogger(DiscretizedPairReduceByKeyBolt.class);
    private static final long serialVersionUID = -7315535125329004118L;
    private ReduceFunction<T> fun;
    private Map<Integer, T> slideDataMap;

    public DiscretizedReduceBolt(ReduceFunction<T> function, String preComponentId) {
        super(preComponentId);
        this.fun = function;
        slideDataMap = new HashMap<>(BUFFER_SLIDES_NUM);
    }


    @Override
    public void processTuple(Tuple tuple) {
        try{
            int slideId = tuple.getInteger(0);
            slideId = slideId%BUFFER_SLIDES_NUM;
            T value = (T) tuple.getValue(1);

            T reducedValue = slideDataMap.get(slideId);
            if(null == reducedValue){
                reducedValue = value;
                slideDataMap.put(slideId, reducedValue);
            } else {
                slideDataMap.put(slideId, fun.reduce(reducedValue, value));
            }
        } catch (Exception e) {
            logger.error(e.toString());
        }
    }

    @Override
    public void processSlide(BasicOutputCollector collector, int slideIndex) {
        T t = slideDataMap.get(slideIndex);
        if(null != t){
            collector.emit(new Values(slideIndex, t));
        }
        // clear data
        slideDataMap.put(slideIndex, null);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        super.declareOutputFields(declarer);
        declarer.declareStream(Utils.DEFAULT_STREAM_ID,
                new Fields(BoltConstants.OutputSlideIdField, BoltConstants.OutputValueField));
    }
}
