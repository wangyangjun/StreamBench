package fi.aalto.dmg.frame.bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import fi.aalto.dmg.frame.functions.FlatMapFunction;
import fi.aalto.dmg.statistics.Throughput;
import org.apache.log4j.Logger;

/**
 * Created by yangjun.wang on 01/11/15.
 */
public class FlatMapBolt<T, R> extends BaseBasicBolt {

    private static final Logger logger = Logger.getLogger(FlatMapBolt.class);
    private static final long serialVersionUID = -1702029689864457150L;

    private FlatMapFunction<T, R> fun;
    private Throughput throughput;

    public FlatMapBolt(FlatMapFunction<T, R> function){
        this.fun = function;
    }

    public FlatMapBolt(FlatMapFunction<T, R> function, Logger logger){
        this(function);
        this.throughput = new Throughput(logger);
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if(null != throughput) {
           throughput.execute();
        }
        Object o = input.getValue(0);
        try {
            Iterable<R> results = this.fun.flatMap((T) o);
            for(R r : results){
                collector.emit(new Values(r));
            }
        } catch (ClassCastException e){
            logger.error("Cast tuple[0] failed");
        } catch (Exception e) {
            logger.error("execute exception: " + e.toString());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(BoltConstants.OutputValueField));
    }
}
