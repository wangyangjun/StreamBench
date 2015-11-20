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
 * Created by jun on 11/9/15.
 */
public class FlatMapValueBolt<V, R> extends BaseBasicBolt {

    private static final Logger logger = LoggerFactory.getLogger(FlatMapValueBolt.class);
    private static final long serialVersionUID = 8601926877987440101L;
    FlatMapFunction<V, R> fun;

    public FlatMapValueBolt(FlatMapFunction<V, R> function){
        this.fun = function;
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
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
