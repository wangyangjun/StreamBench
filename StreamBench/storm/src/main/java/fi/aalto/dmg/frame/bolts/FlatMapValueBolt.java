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
 * Created by jun on 11/9/15.
 */
public class FlatMapValueBolt<V, R> extends BaseBasicBolt {

    private static final Logger logger = Logger.getLogger(FlatMapValueBolt.class);
    private static final long serialVersionUID = 8601926877987440101L;

    private FlatMapFunction<V, R> fun;
    private Throughput throughput;

    public FlatMapValueBolt(FlatMapFunction<V, R> function){
        this.fun = function;
    }

    public void enableThroughput(String loggerName) {
        this.throughput = new Throughput(loggerName);
    }


    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if(null != throughput){
            throughput.execute();
        }
        Object o = input.getValue(1);
        try {
            Iterable<R> results = this.fun.flatMap((V) o);
            for(R r : results){
                collector.emit(new Values(input.getValue(0), r));
            }
        } catch (ClassCastException e){
            logger.error("Cast tuple[1] failed");
        } catch (Exception e) {
            logger.error("execute exception: " + e.toString());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer collector) {
        collector.declare(new Fields(BoltConstants.OutputKeyField, BoltConstants.OutputValueField));
    }
}
