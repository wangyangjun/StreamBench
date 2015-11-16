package fi.aalto.dmg.frame.bolts.windowed;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import fi.aalto.dmg.exceptions.DurationException;
import fi.aalto.dmg.frame.bolts.BoltConstants;
import fi.aalto.dmg.frame.functions.MapFunction;
import fi.aalto.dmg.util.TimeDurations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jun on 11/13/15.
 */
public class WindowMapBolt<T, R> extends WindowedBolt {
    private static final Logger logger = LoggerFactory.getLogger(WindowMapBolt.class);

    // each slide has a corresponding List<R>
    private List<List<R>> mapedList;
    private MapFunction<T, R> fun;

    public WindowMapBolt(MapFunction<T, R> function, TimeDurations windowDuration, TimeDurations slideDuration) throws DurationException {
        super(windowDuration, slideDuration);
        this.fun = function;
        mapedList = new ArrayList<>(WINDOW_SIZE);
        for(int i=0; i<WINDOW_SIZE; ++i){
            mapedList.add(i, new ArrayList<R>());
        }
    }

    /**
     * Map T(t) to R(r) and added it to current slide
     * @param tuple
     */
    @Override
    public void processTuple(Tuple tuple) {
        try{
            List<R> list = mapedList.get(slideInWindow);
            T value = (T)tuple.getValue(0);
            list.add(fun.map(value));
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
            for(List<R> list : mapedList){
                for(R r : list){
                    collector.emit(new Values(slideIndexInBuffer, r));
                }
            }
            // clear data
            mapedList.get((slideInWindow +1)% WINDOW_SIZE).clear();
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
