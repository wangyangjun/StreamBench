package fi.aalto.dmg.frame.bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import fi.aalto.dmg.frame.functions.MapFunction;
import fi.aalto.dmg.statistics.Throughput;
import org.apache.log4j.Logger;

/**
 * Created by yangjun.wang on 31/10/15.
 */
public class MapBolt<T, R> extends BaseBasicBolt {

    private static final Logger logger = Logger.getLogger(MapBolt.class);
    private static final long serialVersionUID = -8469210976959462079L;

    private MapFunction<T, R> fun;
    private Throughput throughput;

    public MapBolt(MapFunction<T, R> function){
        this.fun = function;
    }

    public void enableThroughput(String loggerName) {
        this.throughput = new Throughput(loggerName);
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if(null != throughput) {
            throughput.execute();
        }
        Object o = input.getValue(0);
        try {
            R result = this.fun.map((T) o);
            collector.emit(new Values(result));
        } catch (ClassCastException e){
            logger.error("Cast tuple[0] failed");
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(BoltConstants.OutputValueField));
    }
}
