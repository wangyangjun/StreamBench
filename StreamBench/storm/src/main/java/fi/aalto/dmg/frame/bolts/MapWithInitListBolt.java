package fi.aalto.dmg.frame.bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import fi.aalto.dmg.frame.functions.MapWithInitListFunction;
import fi.aalto.dmg.statistics.ThroughputLog;
import org.apache.log4j.Logger;

import java.util.List;

/**
 * Created by jun on 27/02/16.
 */

public class MapWithInitListBolt<T, R> extends BaseBasicBolt {

    private static final Logger logger = Logger.getLogger(MapBolt.class);
    private static final long serialVersionUID = -8469210976959462079L;

    private MapWithInitListFunction<T, R> fun;
    private ThroughputLog throughput;
    private List<T> initList;

    public MapWithInitListBolt(MapWithInitListFunction<T, R> function, List<T> list) {
        this.fun = function;
        this.initList = list;
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
            R result = this.fun.map((T) o, initList);
            if (null != result) {
                collector.emit(new Values(result));
            }
        } catch (ClassCastException e) {
            logger.error("Cast tuple[0] failed");
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(BoltConstants.OutputValueField));
    }
}
