package fi.aalto.dmg.frame.bolts.windowed;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import fi.aalto.dmg.exceptions.DurationException;
import fi.aalto.dmg.frame.bolts.BoltConstants;
import fi.aalto.dmg.frame.functions.MapPartitionFunction;
import fi.aalto.dmg.util.TimeDurations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * collect data together, then map partition
 * Created by jun on 11/13/15.
 */

public class WindowMapPartitionBolt<T, R> extends WindowedBolt {
    private static final Logger logger = LoggerFactory.getLogger(WindowMapBolt.class);

    // each slide has a corresponding List<R>
    private List<List<T>> mapedList;
    private MapPartitionFunction<T, R> fun;

    public WindowMapPartitionBolt(MapPartitionFunction<T, R> function, TimeDurations windowDuration, TimeDurations slideDuration) throws DurationException {
        super(windowDuration, slideDuration);
        this.fun = function;
        mapedList = new ArrayList<>(WINDOW_SIZE);
        for(int i=0; i<WINDOW_SIZE; ++i){
            mapedList.add(i, new ArrayList<T>());
        }
    }

    /**
     * Map T(t) to R(r) and added it to current slide
     * @param tuple
     */
    @Override
    public void processTuple(Tuple tuple) {
        try{
            List<T> list = mapedList.get(slideInWindow);
            T value = (T)tuple.getValue(0);
            list.add(value);
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
            List<T> windowList = new ArrayList<>();
            for(List<T> list : mapedList){
                windowList.addAll(list);
            }
            Iterable<R> list = fun.mapPartition(windowList);
            for(R r : list){
                collector.emit(new Values(slideIndexInBuffer, r));
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
