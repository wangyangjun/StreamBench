package fi.aalto.dmg.frame.bolts.windowed;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import fi.aalto.dmg.exceptions.DurationException;
import fi.aalto.dmg.frame.bolts.BoltConstants;
import fi.aalto.dmg.frame.functions.FilterFunction;
import fi.aalto.dmg.statistics.ThroughputLog;
import fi.aalto.dmg.util.TimeDurations;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jun on 11/13/15.
 */
public class WindowFilterBolt<T> extends WindowedBolt {
    private static final Logger logger = Logger.getLogger(WindowMapBolt.class);
    private static final long serialVersionUID = 2282056746485449714L;

    // each slide has a corresponding List<R>
    private List<List<T>> filteredList;
    private FilterFunction<T> fun;


    public WindowFilterBolt(FilterFunction<T> function, TimeDurations windowDuration, TimeDurations slideDuration) throws DurationException {
        super(windowDuration, slideDuration);
        this.fun = function;
        filteredList = new ArrayList<>(WINDOW_SIZE);
        for(int i=0; i<WINDOW_SIZE; ++i){
            filteredList.add(i, new ArrayList<T>());
        }
    }

    public void enableThroughput(String loggerName) {
        this.throughput = new ThroughputLog(loggerName);
    }

    /**
     * added filterd value to current slide
     * @param tuple
     */
    @Override
    public void processTuple(Tuple tuple) {
        try{
            List<T> list = filteredList.get(slideInWindow);
            T value = (T)tuple.getValue(0);
            if(fun.filter(value)){
                list.add(value);
            }
        } catch (Exception e) {
            logger.error(e.toString());
        }
    }

    /**
     * emit all the data(type R) in the current window to next component
     * @param collector
     */
    @Override
    public void processSlide(BasicOutputCollector collector) {
        try{
            for(List<T> list : filteredList){
                for(T t : list){
                    collector.emit(new Values(slideIndexInBuffer, t));
                }
            }
            // clear data
            filteredList.get((slideInWindow + 1) % WINDOW_SIZE).clear();
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
