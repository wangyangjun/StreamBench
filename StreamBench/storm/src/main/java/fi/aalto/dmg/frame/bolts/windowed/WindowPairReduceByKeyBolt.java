package fi.aalto.dmg.frame.bolts.windowed;

import backtype.storm.metric.SystemBolt;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import fi.aalto.dmg.exceptions.DurationException;
import fi.aalto.dmg.frame.bolts.BoltConstants;
import fi.aalto.dmg.frame.functions.ReduceFunction;
import fi.aalto.dmg.util.BTree;
import fi.aalto.dmg.util.TimeDurations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Online calculate(default)/ Cumulative data
 * Created by jun on 11/9/15.
 * Tested
 */
public class WindowPairReduceByKeyBolt<K,V> extends WindowedBolt {

    private static final Logger logger = LoggerFactory.getLogger(WindowPairReduceByKeyBolt.class);
    // for each slide, there is a corresponding reduced Tuple2
    private ReduceFunction<V> fun;
    private BTree<Map<K, V>> reduceDataContainer;

    public WindowPairReduceByKeyBolt(ReduceFunction<V> function, TimeDurations windowDuration, TimeDurations slideDuration) throws DurationException {
        super(windowDuration, slideDuration);
        this.fun = function;
        reduceDataContainer = new BTree<>(WINDOW_SIZE);
        for(int i=0; i<WINDOW_SIZE; ++i) {
            reduceDataContainer.set(i, new HashMap<K, V>());
        }
    }

    /**
     * called after receiving a normal tuple
     * @param tuple
     */
    @Override
    public void processTuple(Tuple tuple) {
        try {
            Map<K, V> map = reduceDataContainer.get(slideInWindow);
            K key = (K)tuple.getValue(0);
            V value = (V)tuple.getValue(1);
            V reducedValue = map.get(key);
            if (null == reducedValue)
                map.put(key, value);
            else{
                map.put(key, fun.reduce(reducedValue, value));
            }
            reduceDataContainer.set(slideInWindow, map);
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
            // update slideInWindow node and its parents until root
            // single slide window
            if(!reduceDataContainer.isRoot(slideInWindow)) {
                int updatedNode = slideInWindow;
                // if latest updated node is not root, update its parent node
                while (!reduceDataContainer.isRoot(updatedNode)){
                    int parent = reduceDataContainer.findParent(updatedNode);
                    BTree.Children children = reduceDataContainer.findChildren(parent);
                    reduceDataContainer.set(parent,
                            merge(reduceDataContainer.get(children.getChild1()),
                                    reduceDataContainer.get(children.getChild2())));
                    updatedNode = parent;
                }
            }
            Map<K,V> root = reduceDataContainer.getRoot();
            for(Map.Entry<K, V> entry: root.entrySet()){
                collector.emit(new Values(slideIndexInBuffer, entry.getKey(), entry.getValue()));
            }
            // clear data
            reduceDataContainer.get((slideInWindow + 1) % WINDOW_SIZE).clear();
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.toString());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        super.declareOutputFields(declarer);
        declarer.declare(new Fields(BoltConstants.OutputSlideIdField, BoltConstants.OutputKeyField, BoltConstants.OutputValueField));
    }

    private Map<K, V> merge(Map<K,V> leftMap, Map<K,V> rightMap) throws Exception {
        Map<K,V> parentMap = new HashMap<>(leftMap);
        for(Map.Entry<K, V> entry: rightMap.entrySet()){
            if(parentMap.containsKey(entry.getKey())){
                parentMap.put(entry.getKey(), fun.reduce(parentMap.get(entry.getKey()), entry.getValue() ));
            } else {
                parentMap.put(entry.getKey(), entry.getValue());
            }
        }
        return parentMap;
    }

}
