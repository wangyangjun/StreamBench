package fi.aalto.dmg.frame.bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import fi.aalto.dmg.statistics.LatencyLog;
import fi.aalto.dmg.statistics.ThroughputLog;
import fi.aalto.dmg.util.WithTime;
import org.apache.log4j.Logger;

/**
 * Created by jun on 08/12/15.
 */
public class PairLatencyBolt<T> extends BaseBasicBolt {

    private static final long serialVersionUID = 5063888858772660110L;

    private static final Logger logger = Logger.getLogger(PairLatencyBolt.class);
    private LatencyLog latency;
    private ThroughputLog throughput;

    public PairLatencyBolt() {
        latency = new LatencyLog(this.getClass().getName());
    }

    public void enableThroughput(String loggerName) {
        this.throughput = new ThroughputLog(loggerName);
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
