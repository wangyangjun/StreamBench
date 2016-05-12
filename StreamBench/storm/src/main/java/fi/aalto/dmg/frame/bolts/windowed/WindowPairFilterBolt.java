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
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jun on 11/13/15.
 */

public class WindowPairFilterBolt<K, V> extends WindowedBolt {
    private static final Logger logger = Logger.getLogger(WindowMapBolt.class);
    private static final long serialVersionUID = 1946088053712015357L;

    // each slide has a corresponding List<R>
    private List<List<Tuple2<K, V>>> filteredList;
    private FilterFunction<Tuple2<K, V>> fun;

    public WindowPairFilterBolt(FilterFunction<Tuple2<K, V>> function,
                                TimeDurations windowDuration,
                                TimeDurations slideDuration) throws DurationException {
        super(windowDuration, slideDuration);
        this.fun = function;
        filteredList = new ArrayList<>(WINDOW_SIZE);
        for (int i = 0; i < WINDOW_SIZE; ++i) {
            filteredList.add(i, new ArrayList<Tuple2<K, V>>());
        }
    }

    public void enableThroughput(String loggerName) {
        this.throughput = new ThroughputLog(loggerName);
    }

    /**
     * added filterd value to current slide
     *
     * @param tuple
     */
    @Override
    public void processTuple(Tuple tuple) {
        try {
            List<Tuple2<K, V>> list = filteredList.get(slideInWindow);
            K key = (K) tuple.getValue(0);
            V value = (V) tuple.getValue(1);
            Tuple2<K, V> tuple2 = new Tuple2<>(key, value);
            if (fun.filter(tuple2)) {
                list.add(tuple2);
            }
        } catch (Exception e) {
            logger.error(e.toString());
        }
    }

    /**
     * emit all the data(type R) in the current window to next component
     *
     * @param collector
     */
    @Override
    public void processSlide(BasicOutputCollector collector) {
        try {
            for (List<Tuple2<K, V>> list : filteredList) {
                for (Tuple2<K, V> t : list) {
                    collector.emit(new Values(slideIndexInBuffer, t._1(), t._2()));
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
        declarer.declare(new Fields(BoltConstants.OutputSlideIdField, BoltConstants.OutputKeyField, BoltConstants.OutputValueField));
    }
}
