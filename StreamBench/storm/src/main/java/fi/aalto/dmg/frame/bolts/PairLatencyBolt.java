package fi.aalto.dmg.frame.bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import fi.aalto.dmg.statistics.Latency;
import fi.aalto.dmg.util.WithTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jun on 08/12/15.
 */
public class PairLatencyBolt<T> extends BaseBasicBolt {

    private static final long serialVersionUID = 5063888858772660110L;

    private static final Logger logger = LoggerFactory.getLogger(PairLatencyBolt.class);
    private Latency latency;

    public PairLatencyBolt() {
        latency = new Latency(logger);
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        WithTime<T> withTime = (WithTime<T>)input.getValue(1);
        latency.execute(withTime);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

}
