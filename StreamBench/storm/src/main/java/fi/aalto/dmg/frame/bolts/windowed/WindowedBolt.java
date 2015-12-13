package fi.aalto.dmg.frame.bolts.windowed;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import fi.aalto.dmg.exceptions.DurationException;
import fi.aalto.dmg.frame.bolts.BoltConstants;
import fi.aalto.dmg.statistics.Throughput;
import fi.aalto.dmg.util.TimeDurations;
import fi.aalto.dmg.util.Utils;

import java.util.Map;

/**
 * Created by jun on 11/12/15.
 */
public abstract class WindowedBolt extends BaseBasicBolt {

    private static final long serialVersionUID = 1571515050457013114L;
    // topology tick tuple frequency in seconds
    private long TOPOLOGY_TICK_TUPLE_FREQ_SECS;

    // slides id emit to next component 0,1,2
    protected static int BUFFER_SLIDES_NUM = 3;
    protected static int slideIndexInBuffer = 0;

    // Slides number in a window
    protected int WINDOW_SIZE;
    // slide index in the windowed data structure: (0, 1, ..., WINDOW_SIZE-1)
    protected int slideInWindow = 0;

    protected Throughput throughput;

    public WindowedBolt(TimeDurations windowDuration, TimeDurations slideDuration) throws DurationException {
        long window_tick_frequency_seconds = Utils.getSeconds(windowDuration);
        long slide_tick_frequency_seconds = Utils.getSeconds(slideDuration);
        this.TOPOLOGY_TICK_TUPLE_FREQ_SECS = slide_tick_frequency_seconds;

        // window duration should be multi of slide duration
        if(window_tick_frequency_seconds % slide_tick_frequency_seconds != 0){
            throw new DurationException("Window duration should be multi times of slide duration.");
        }
        WINDOW_SIZE = (int) (window_tick_frequency_seconds / slide_tick_frequency_seconds);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        if(null != throughput) {
            throughput.execute();
        }
        if(isTickTuple(tuple)){
            processSlide(collector);
            collector.emit(BoltConstants.TICK_STREAM_ID, new Values(slideIndexInBuffer));
            slideIndexInBuffer = (slideIndexInBuffer +1)%BUFFER_SLIDES_NUM;
            slideInWindow = (slideInWindow +1)% WINDOW_SIZE;
        } else {
            processTuple(tuple);
        }
    }

    public abstract void processTuple(Tuple tuple);
    public abstract void processSlide(BasicOutputCollector collector);

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, TOPOLOGY_TICK_TUPLE_FREQ_SECS);
        return conf;
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


    public static boolean isTickTuple(Tuple tuple){
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }
}
