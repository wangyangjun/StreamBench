package fi.aalto.dmg.frame.bolts.windowed;

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

/**
 * Created by jun on 11/13/15.
 */

public class WindowPairReduceBolt<K,V> extends WindowedBolt {

    private static final Logger logger = LoggerFactory.getLogger(WindowPairReduceByKeyBolt.class);
    // for each slide, there is a corresponding reduced Tuple2
    private BTree<Tuple2<K,V>> reduceDataContainer;
    private ReduceFunction<Tuple2<K, V>> fun;

    public WindowPairReduceBolt(ReduceFunction<Tuple2<K, V>> function, TimeDurations windowDuration, TimeDurations slideDuration) throws DurationException {
        super(windowDuration, slideDuration);
        this.fun = function;
        reduceDataContainer = new BTree<>(WINDOW_SIZE);
    }

    /**
     * called after receiving a normal tuple
     * @param tuple
     */
    @Override
    public void processTuple(Tuple tuple) {
        try {
            Tuple2<K,V> reducedTuple = reduceDataContainer.get(slideInWindow);
            K key = (K)tuple.getValue(0);
            V value = (V)tuple.getValue(1);
            Tuple2<K,V> tuple2 = new Tuple2<>(key, value);
            if (null == reducedTuple)
                reducedTuple = tuple2;
            else{
                reducedTuple = fun.reduce(reducedTuple, tuple2);
            }
            reduceDataContainer.set(slideInWindow, reducedTuple);
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
            if(reduceDataContainer.isRoot(slideInWindow)){
                collector.emit(new Values(slideIndexInBuffer,
                        reduceDataContainer.get(slideInWindow)._1(), reduceDataContainer.get(slideInWindow)._2()));
            } else {
                int updatedNode = slideInWindow;
                // if latest updated node is not root, update its parent node
                while (!reduceDataContainer.isRoot(updatedNode)){
                    int parent = reduceDataContainer.findParent(updatedNode);
                    BTree.Children children = reduceDataContainer.findChildren(parent);
                    if(null == reduceDataContainer.get(children.getChild1())
                            || null == reduceDataContainer.get(children.getChild2())) {
                        reduceDataContainer.set(parent, reduceDataContainer.get(updatedNode));
                    } else {
                        reduceDataContainer.set(parent,
                                fun.reduce(reduceDataContainer.get(children.getChild1()), reduceDataContainer.get(children.getChild2())));
                    }
                    updatedNode = parent;
                }
            }
            Tuple2<K,V> root = reduceDataContainer.getRoot();
            collector.emit(new Values(slideIndexInBuffer, root._1(), root._2()));
            // clear data
            reduceDataContainer.set((slideInWindow + 1) % WINDOW_SIZE, null);
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
