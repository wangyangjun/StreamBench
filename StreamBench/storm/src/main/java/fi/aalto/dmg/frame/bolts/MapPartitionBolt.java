package fi.aalto.dmg.frame.bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import fi.aalto.dmg.frame.functions.MapPartitionFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yangjun.wang on 31/10/15.
 */
public class MapPartitionBolt<T,R> extends BaseBasicBolt {

    private static final Logger logger = LoggerFactory.getLogger(MapPartitionBolt.class);

    MapPartitionFunction<T, R> fun;

    public MapPartitionBolt(MapPartitionFunction<T, R> function){
        this.fun = function;
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields());
    }
}
