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
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jun on 11/13/15.
 */
public class DiscretizedPairMapPartitionBolt<K, V, R> extends DiscretizedBolt {
    private static final Logger logger = LoggerFactory.getLogger(DiscretizedMapBolt.class);
    private static final long serialVersionUID = -7515076040626001607L;

    // store received data without any process
    private Map<Integer, List<Tuple2<K, V>>> slideDataMap;
    private MapPartitionFunction<Tuple2<K, V>, Tuple2<K, R>> fun;

    public DiscretizedPairMapPartitionBolt(MapPartitionFunction<Tuple2<K, V>, Tuple2<K, R>> function, String preComponentId) {
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
        K key = (K) tuple.getValue(1);
        V value = (V) tuple.getValue(2);
        List<Tuple2<K, V>> mapedList = slideDataMap.get(slideId);
        if (null == mapedList) {
            mapedList = new ArrayList<>();
        }
        mapedList.add(new Tuple2<>(key, value));
        slideDataMap.put(slideId, mapedList);
    }

    @Override
    public void processSlide(BasicOutputCollector collector, int slideIndex) {
        List<Tuple2<K, V>> list = slideDataMap.get(slideIndex);
        Iterable<Tuple2<K, R>> results = fun.mapPartition(list);
        for (Tuple2<K, R> tuple2 : results) {
            collector.emit(new Values(slideIndex, tuple2._1(), tuple2._2()));
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
