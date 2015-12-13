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
 * Created by jun on 11/9/15.
 */

public class MapValueBolt<V, R> extends BaseBasicBolt {

    private static final Logger logger = Logger.getLogger(MapBolt.class);
    private static final long serialVersionUID = 8892670349365399357L;

    private MapFunction<V, R> fun;
    private Throughput throughput;

    public MapValueBolt(MapFunction<V, R> function){
        this.fun = function;
    }

    public MapValueBolt(MapFunction<V, R> function, Logger logger){
        this(function);
        this.throughput = new Throughput(logger);
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        throughput.execute();
        Object o = input.getValue(1);
        try {
            R result = this.fun.map((V) o);
            collector.emit(new Values(input.getValue(0), result));
        } catch (ClassCastException e){
            logger.error("Cast tuple[0] failed");
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(BoltConstants.OutputKeyField, BoltConstants.OutputValueField));
    }
}
