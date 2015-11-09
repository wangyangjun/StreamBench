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
 * Created by jun on 11/9/15.
 */
public class WindowReduceBolt<T> extends BaseBasicBolt {
    private static final Logger logger = LoggerFactory.getLogger(WindowReduceBolt.class);
    private List<T> reduceList;
    private long WINDOW_TICK_FREQUENCY_SECONDES;
    private long SLIDE_TICK_FREQUENCY_SECONDES;
    private int slides;
    private int current_silde;

    ReduceFunction<T> fun;

    public WindowReduceBolt(ReduceFunction<T> function, TimeDurations windowDuration, TimeDurations slideDuration) throws DurationException {
        this.fun = function;
        WINDOW_TICK_FREQUENCY_SECONDES = Utils.getSeconds(windowDuration);
        SLIDE_TICK_FREQUENCY_SECONDES = Utils.getSeconds(slideDuration);
        if(WINDOW_TICK_FREQUENCY_SECONDES%SLIDE_TICK_FREQUENCY_SECONDES != 0){
            throw new DurationException("Window duration should be multi times of slide duration.");
        }
        slides = (int) (WINDOW_TICK_FREQUENCY_SECONDES/SLIDE_TICK_FREQUENCY_SECONDES);
        current_silde = 0;
        reduceList = new ArrayList<>(slides);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        try {
            if(isTickTuple(tuple)){
                T reduceValue = null;
                for(T t : reduceList){
                    if( null == reduceValue){
                        reduceValue = t;
                    } else {
                        reduceValue = fun.reduce(reduceValue, t);
                    }
                }
                collector.emit(new Values(reduceValue));
                current_silde = (current_silde+1)%slides;
                reduceList.set(current_silde, null);
            } else {
                T reduceValue = reduceList.get(current_silde);
                T value = (T)tuple.getValue(0);
                if (null == reduceValue)
                    reduceList.set(current_silde, value);
                else{
                    reduceList.set(current_silde, fun.reduce(reduceValue, value));
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(BoltConstants.OutputValueField));
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
