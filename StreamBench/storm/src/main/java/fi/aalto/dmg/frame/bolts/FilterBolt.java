package fi.aalto.dmg.frame.bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import fi.aalto.dmg.frame.functions.FilterFunction;
import fi.aalto.dmg.statistics.ThroughputLog;
import org.apache.log4j.Logger;

/**
 * Created by yangjun.wang on 01/11/15.
 */
public class FilterBolt<T> extends BaseBasicBolt {

    private static final Logger logger = Logger.getLogger(FilterBolt.class);
    private static final long serialVersionUID = 2401869900116259991L;
    FilterFunction<T> fun;
    ThroughputLog throughput;

    public FilterBolt(FilterFunction<T> function) {
        this.fun = function;
    }

    public void enableThroughput(String loggerName) {
        this.throughput = new ThroughputLog(loggerName);
    }


    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if (null != throughput) {
            throughput.execute();
        }
        Object o = input.getValue(0);
        try {
            if (this.fun.filter((T) o)) {
                collector.emit(new Values(o));
            }
        } catch (ClassCastException e) {
            logger.error("Cast tuple[0] failed");
        } catch (Exception e) {
            e.printStackTrace();
            // logger.error("execute exception: " + e.toString());
        }
        logger.error("Hello world");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(BoltConstants.OutputValueField));
    }
}
