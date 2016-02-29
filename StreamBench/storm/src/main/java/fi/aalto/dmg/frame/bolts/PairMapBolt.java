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
import scala.Tuple2;

/**
 * Created by jun on 28/02/16.
 */
public class PairMapBolt<K, V, R> extends BaseBasicBolt {

    private static final Logger logger = Logger.getLogger(MapBolt.class);
    private static final long serialVersionUID = 1L;

    private MapFunction<Tuple2<K,V>, R> fun;
    private Throughput throughput;

    public PairMapBolt(MapFunction<Tuple2<K,V>, R> function){
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
        K key = (K)input.getValue(0);
        V value = (V)input.getValue(1);

        try {
            R result = this.fun.map(new Tuple2<>(key, value));
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
