package fi.aalto.dmg.frame.bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import fi.aalto.dmg.frame.functions.FlatMapPairFunction;
import fi.aalto.dmg.statistics.Throughput;
import org.apache.log4j.Logger;
import scala.Tuple2;

/**
 * Created by jun on 20/12/15.
 */
public class FlatMapToPairBolt<T, K, V> extends BaseBasicBolt {
    private static final Logger logger = Logger.getLogger(MapToPairBolt.class);
    private static final long serialVersionUID = 713275144540880633L;

    private FlatMapPairFunction<T, K, V> fun;
    private Throughput throughput;

    public FlatMapToPairBolt(FlatMapPairFunction<T, K, V> function){
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
            Iterable<Tuple2<K, V>> results = this.fun.flatMapToPair((T) o);
            for(Tuple2<K,V> tuple2 : results){
                collector.emit(new Values(tuple2._1(), tuple2._2()));
            }
        } catch (ClassCastException e){
            logger.error("Cast tuple[0] failed");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(BoltConstants.OutputKeyField, BoltConstants.OutputValueField));
    }
}
