package fi.aalto.dmg.frame.bolts.windowed;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import fi.aalto.dmg.exceptions.DurationException;
import fi.aalto.dmg.frame.bolts.BoltConstants;
import fi.aalto.dmg.frame.functions.MapPairFunction;
import fi.aalto.dmg.statistics.ThroughputLog;
import fi.aalto.dmg.util.TimeDurations;
import org.apache.log4j.Logger;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jun on 11/13/15.
 */
public class WindowMapToPairBolt<T, K, V> extends WindowedBolt {
    private static final Logger logger = Logger.getLogger(WindowMapBolt.class);
    private static final long serialVersionUID = -287565768441900556L;

    // each slide has a corresponding List<R>
    private List<List<Tuple2<K, V>>> mapedList;
    private MapPairFunction<T, K, V> fun;

    public WindowMapToPairBolt(MapPairFunction<T, K, V> function,
                               TimeDurations windowDuration,
                               TimeDurations slideDuration) throws DurationException {
        super(windowDuration, slideDuration);
        this.fun = function;
        mapedList = new ArrayList<>(WINDOW_SIZE);
        for (int i = 0; i < WINDOW_SIZE; ++i) {
            mapedList.add(i, new ArrayList<Tuple2<K, V>>());
        }
    }

    public void enableThroughput(String loggerName) {
        this.throughput = new ThroughputLog(loggerName);
    }


    /**
     * Map T(t) to R(r) and added it to current slide
     *
     * @param tuple
     */
    @Override
    public void processTuple(Tuple tuple) {
        try {
            List<Tuple2<K, V>> list = mapedList.get(slideInWindow);
            T value = (T) tuple.getValue(0);
            list.add(fun.mapToPair(value));
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
            for (List<Tuple2<K, V>> list : mapedList) {
                for (Tuple2<K, V> r : list) {
                    collector.emit(new Values(slideIndexInBuffer, r._1(), r._2()));
                }
            }
            // clear data
            mapedList.get((slideInWindow + 1) % WINDOW_SIZE).clear();
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
