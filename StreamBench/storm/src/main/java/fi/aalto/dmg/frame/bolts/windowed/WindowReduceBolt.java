package fi.aalto.dmg.frame.bolts.windowed;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import fi.aalto.dmg.exceptions.DurationException;
import fi.aalto.dmg.frame.bolts.BoltConstants;
import fi.aalto.dmg.frame.functions.ReduceFunction;
import fi.aalto.dmg.util.TimeDurations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jun on 11/9/15.
 */
public class WindowReduceBolt<T> extends WindowedBolt {
    private static final Logger logger = LoggerFactory.getLogger(WindowReduceBolt.class);
    private static final long serialVersionUID = 865100279347059333L;

    // window data structure TODO: replace it with tree
    private List<T> reduceList;
    private ReduceFunction<T> fun;

    public WindowReduceBolt(ReduceFunction<T> function, TimeDurations windowDuration, TimeDurations slideDuration) throws DurationException {
        super(windowDuration, slideDuration);
        this.fun = function;
        reduceList = new ArrayList<>(WINDOW_SIZE);
    }

    @Override
    public void processTuple(Tuple tuple) {
        try{
            T reduceValue = reduceList.get(slideInWindow);
            T value = (T)tuple.getValue(0);
            if (null == reduceValue)
                reduceList.set(slideInWindow, value);
            else{
                reduceList.set(slideInWindow, fun.reduce(reduceValue, value));
            }
        } catch (Exception e) {
            logger.error(e.toString());
        }

    }

    @Override
    public void processSlide(BasicOutputCollector collector) {
        try{
            T reduceValue = null;
            // TODO: implement window data structure with tree, no need to for loop
            for(T t : reduceList){
                if( null == reduceValue){
                    reduceValue = t;
                } else {
                    reduceValue = fun.reduce(reduceValue, t);
                }
            }
            collector.emit(new Values(slideIndexInBuffer, reduceValue));
            // clear data
            reduceList.set((slideInWindow +1)% WINDOW_SIZE, null);
        } catch (Exception e) {
            logger.error(e.toString());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        super.declareOutputFields(declarer);
        declarer.declare(new Fields(BoltConstants.OutputSlideIdField, BoltConstants.OutputValueField));
    }


}
