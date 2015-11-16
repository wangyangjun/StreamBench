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
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jun on 11/13/15.
 */

public class WindowPairReduceBolt<K,V> extends WindowedBolt {

    private static final Logger logger = LoggerFactory.getLogger(WindowPairReduceByKeyBolt.class);
    // for each slide, there is a corresponding reduced Tuple2
    private List<Tuple2<K,V>> reduceList;
    private ReduceFunction<Tuple2<K, V>> fun;

    public WindowPairReduceBolt(ReduceFunction<Tuple2<K, V>> function, TimeDurations windowDuration, TimeDurations slideDuration) throws DurationException {
        super(windowDuration, slideDuration);
        this.fun = function;
        reduceList = new ArrayList<>(WINDOW_SIZE);
    }

    /**
     * called after receiving a normal tuple
     * @param tuple
     */
    @Override
    public void processTuple(Tuple tuple) {
        try {
            Tuple2<K,V> reducedTuple = reduceList.get(sildeInWindow);
            K key = (K)tuple.getValue(0);
            V value = (V)tuple.getValue(1);
            Tuple2<K,V> tuple2 = new Tuple2<>(key, value);
            if (null == reducedTuple)
                reducedTuple = tuple2;
            else{
                reducedTuple = fun.reduce(reducedTuple, tuple2);
            }
            reduceList.add(sildeInWindow, reducedTuple);
        } catch (Exception e) {
            logger.error(e.toString());
        }
    }

    /**
     * called after receiving a tick tuple
     * reduce all data(slides) in current window
     * @param collector
     */
    @Override
    public void processSlide(BasicOutputCollector collector) {
        try{
            Tuple2<K,V> reducedWindowTuple = null;
            for(Tuple2<K,V> tuple : reduceList){
                if(null == reducedWindowTuple){
                    reducedWindowTuple = tuple;
                } else {
                    reducedWindowTuple = fun.reduce(reducedWindowTuple, tuple);
                }
            }
            collector.emit(new Values(slideIndexInBuffer, reducedWindowTuple._1(), reducedWindowTuple._2()));
            // clear data
            reduceList.add((sildeInWindow + 1) % WINDOW_SIZE, null);
        } catch (Exception e) {
            logger.error(e.toString());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        super.declareOutputFields(declarer);
        declarer.declare(new Fields(BoltConstants.OutputSlideIdField, BoltConstants.OutputKeyField, BoltConstants.OutputValueField));
    }

}