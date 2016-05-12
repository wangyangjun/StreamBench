package fi.aalto.dmg.frame.bolts.discretized;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import fi.aalto.dmg.frame.bolts.BoltConstants;
import fi.aalto.dmg.frame.functions.MapPairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jun on 12/11/15.
 */
public class DiscretizedMapToPairBolt<T, K, V> extends DiscretizedBolt {
    private static final Logger logger = LoggerFactory.getLogger(DiscretizedMapBolt.class);
    private static final long serialVersionUID = 1291962079966581567L;

    private Map<Integer, List<Tuple2<K, V>>> slideDataMap;
    private MapPairFunction<T, K, V> fun;

    public DiscretizedMapToPairBolt(MapPairFunction<T, K, V> function, String preComponentId) {
        super(preComponentId);
        this.fun = function;
        slideDataMap = new HashMap<>(BUFFER_SLIDES_NUM);
        for (int i = 0; i < BUFFER_SLIDES_NUM; ++i) {
            slideDataMap.put(i, new ArrayList<Tuple2<K, V>>());
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
        List<Tuple2<K, V>> pariList = slideDataMap.get(slideId);
        if (null == pariList) {
            pariList = new ArrayList<>();
        }
        pariList.add(fun.mapToPair(t));
        slideDataMap.put(slideId, pariList);
    }

    @Override
    public void processSlide(BasicOutputCollector collector, int slideIndex) {
        List<Tuple2<K, V>> list = slideDataMap.get(slideIndex);
        for (Tuple2<K, V> pair : list) {
            collector.emit(new Values(slideIndex, pair._1(), pair._2()));
        }
        // clear data
        list.clear();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        super.declareOutputFields(declarer);
        declarer.declareStream(Utils.DEFAULT_STREAM_ID,
                new Fields(BoltConstants.OutputSlideIdField, BoltConstants.OutputKeyField, BoltConstants.OutputValueField));
    }
}
