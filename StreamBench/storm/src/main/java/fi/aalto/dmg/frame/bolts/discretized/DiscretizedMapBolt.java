package fi.aalto.dmg.frame.bolts.discretized;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import fi.aalto.dmg.frame.bolts.BoltConstants;
import fi.aalto.dmg.frame.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jun on 11/12/15.
 */
public class DiscretizedMapBolt<T, R> extends DiscretizedBolt {
    private static final Logger logger = LoggerFactory.getLogger(DiscretizedMapBolt.class);
    private static final long serialVersionUID = 7646952300108809739L;

    private Map<Integer, List<R>> slideDataMap;
    private MapFunction<T, R> fun;

    public DiscretizedMapBolt(MapFunction<T, R> function, String preComponentId) {
        super(preComponentId);
        this.fun = function;
        slideDataMap = new HashMap<>(BUFFER_SLIDES_NUM);
        for (int i = 0; i < BUFFER_SLIDES_NUM; ++i) {
            slideDataMap.put(i, new ArrayList<R>());
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
        List<R> mapedList = slideDataMap.get(slideId);
        if (null == mapedList) {
            mapedList = new ArrayList<>();
        }
        mapedList.add(fun.map(t));
        slideDataMap.put(slideId, mapedList);
    }

    @Override
    public void processSlide(BasicOutputCollector collector, int slideIndex) {
        List<R> list = slideDataMap.get(slideIndex);
        for (R r : list) {
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
