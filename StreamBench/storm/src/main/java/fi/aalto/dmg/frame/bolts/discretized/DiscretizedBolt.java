package fi.aalto.dmg.frame.bolts.discretized;


import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import fi.aalto.dmg.frame.bolts.BoltConstants;

import java.util.HashMap;
import java.util.Map;

/**
 * Bolt for discretized stream
 * Created by jun on 11/12/15.
 */
public abstract class DiscretizedBolt extends BaseBasicBolt {

    // the number of buffer slides
    protected static int BUFFER_SLIDES_NUM = 3;
    // Map: slideId -> received tick tuples from the slide
    private Map<Integer, Integer> slideTicksMap;

    private int preComponentTaksNum = 0;
    protected String preComponentId;

    public DiscretizedBolt(String preComponentId){
        this.preComponentId = preComponentId;
        slideTicksMap = new HashMap<>();

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context){
        preComponentTaksNum = context.getComponentTasks(preComponentId).size();

    }

    /**
     * Including logic of determining the end of one slide
     * Before process the slide, all tick tuples of last component(one for each node) must be received
     *
     * @param tuple
     * @param collector
     */
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        // which slide the tuple belongs to
        if(isTickTuple(tuple)){
            int slideId = tuple.getInteger(0);
            int slideIndex = slideId%BUFFER_SLIDES_NUM;
            Integer receivedTicks = slideTicksMap.get(slideIndex);
            if(null == receivedTicks){
                receivedTicks = 1;
            } else {
                receivedTicks++;
            }
            /* check whether is it the last tick for the slide */
            if( receivedTicks == preComponentTaksNum) {
                // process data in the slide and emit to the next component
                processSlide(collector, slideIndex);
                collector.emit(BoltConstants.TICK_STREAM_ID, new Values(slideIndex));
                // clear the slide
                slideTicksMap.put(slideIndex, 0);
            } else {
                slideTicksMap.put(slideIndex, receivedTicks);
            }
        } else {
            processTuple(tuple);
        }
    }

    /**
     * declare tick stream
     * @param declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        /* declare tick stream, tick tuple with id of the latest end slide */
        declarer.declareStream(BoltConstants.TICK_STREAM_ID, new Fields(BoltConstants.OutputSlideIdField));
    }

    /**
     * How to determine which slide the tuple belongs to?
     * @param tuple
     */
    public abstract void processTuple(Tuple tuple);
    public abstract void processSlide(BasicOutputCollector collector, int slideIndex);

    private static boolean isTickTuple(Tuple tuple){
        return tuple.getSourceStreamId().equals(BoltConstants.TICK_STREAM_ID);
    }

}
