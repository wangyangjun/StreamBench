package fi.aalto.dmg.frame.bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import fi.aalto.dmg.statistics.Latency;
import fi.aalto.dmg.statistics.Throughput;
import fi.aalto.dmg.util.WithTime;
import org.apache.log4j.Logger;

/**
 * Created by jun on 08/12/15.
 */
public class PairLatencyBolt<T> extends BaseBasicBolt {

    private static final long serialVersionUID = 5063888858772660110L;

    private static final Logger logger = Logger.getLogger(PairLatencyBolt.class);
    private Latency latency;
    private Throughput throughput;

    public PairLatencyBolt() {
        latency = new Latency(this.getClass().getName());
    }

    public void enableThroughput(String loggerName) {
        this.throughput = new Throughput(loggerName);
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if(null != throughput) {
            throughput.execute();
        }
        WithTime<T> withTime = (WithTime<T>)input.getValue(1);
        latency.execute(withTime);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

}
