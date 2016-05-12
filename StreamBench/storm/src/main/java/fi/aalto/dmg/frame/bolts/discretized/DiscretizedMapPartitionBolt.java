package fi.aalto.dmg.frame.bolts.discretized;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import fi.aalto.dmg.frame.bolts.BoltConstants;
import fi.aalto.dmg.frame.functions.MapPartitionFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Map windowed stream in local node
 * Cumulative or online calculate, MapPartition must be cumulative
 * Created by jun on 11/12/15.
 */
public class DiscretizedMapPartitionBolt<T, R> extends DiscretizedBolt {
    private static final Logger logger = LoggerFactory.getLogger(DiscretizedMapBolt.class);
    private static final long serialVersionUID = -5830804051164631703L;

    // store received data without any process
    private Map<Integer, List<T>> slideDataMap;
    private MapPartitionFunction<T, R> fun;

    public DiscretizedMapPartitionBolt(MapPartitionFunction<T, R> function, String preComponentId) {
        super(preComponentId);
        this.fun = function;
        slideDataMap = new HashMap<>(BUFFER_SLIDES_NUM);
        for (int i = 0; i < BUFFER_SLIDES_NUM; ++i) {
            slideDataMap.put(i, new ArrayList<T>());
        }
    }

    /**
     * determine which slide the tuple belongs to
     *
     * @param tuple
     */
    @Override
    public void processTuple(Tuple tuple) {
        int slideId = tuple.getInteger(0);
        slideId = slideId % BUFFER_SLIDES_NUM;
        T t = (T) tuple.getValue(1);
        List<T> mapedList = slideDataMap.get(slideId);
        if (null == mapedList) {
            mapedList = new ArrayList<>();
        }
        mapedList.add(t);
        slideDataMap.put(slideId, mapedList);
    }

    @Override
    public void processSlide(BasicOutputCollector collector, int slideIndex) {
        List<T> list = slideDataMap.get(slideIndex);
        Iterable<R> results = fun.mapPartition(list);
        for (R r : results) {
            collector.emit(new Values(slideIndex, r));
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
