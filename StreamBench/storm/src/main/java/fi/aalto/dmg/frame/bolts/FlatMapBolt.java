package fi.aalto.dmg.frame.bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import fi.aalto.dmg.frame.functions.FlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yangjun.wang on 01/11/15.
 */
public class FlatMapBolt<T, R> extends BaseBasicBolt {

    private static final Logger logger = LoggerFactory.getLogger(FlatMapBolt.class);
    FlatMapFunction<T, R> fun;

    public FlatMapBolt(FlatMapFunction<T, R> function){
        this.fun = function;
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
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
        declarer.declare(new Fields("key"));
    }
}
