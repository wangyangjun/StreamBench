package fi.aalto.dmg.frame.bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import fi.aalto.dmg.frame.functions.FilterFunction;
import fi.aalto.dmg.statistics.Throughput;
import org.apache.log4j.Logger;
import scala.Tuple2;


/**
 * Created by jun on 11/9/15.
 */

public class PairFilterBolt<K,V> extends BaseBasicBolt {

    private static final Logger logger = Logger.getLogger(FilterBolt.class);
    private static final long serialVersionUID = -3959145878061949596L;

    private FilterFunction<Tuple2<K,V>> fun;
    private Throughput throughput;

    public PairFilterBolt(FilterFunction<Tuple2<K, V>> function){
        this.fun = function;
    }

    public PairFilterBolt(FilterFunction<Tuple2<K, V>> function, Logger logger){
        this(function);
        this.throughput = new Throughput(logger);
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        Object k = input.getValue(0);
        Object v = input.getValue(1);
        try {
            if(this.fun.filter(new Tuple2<>((K) k, (V) v))){
                collector.emit(new Values(k, v));
            }
        } catch (ClassCastException e){
            logger.error("Cast tuple failed");
        } catch (Exception e) {
            e.printStackTrace();
            // logger.error("execute exception: " + e.toString());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(BoltConstants.OutputKeyField, BoltConstants.OutputValueField));
    }
}
