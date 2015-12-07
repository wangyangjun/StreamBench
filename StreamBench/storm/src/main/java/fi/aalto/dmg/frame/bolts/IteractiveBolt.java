package fi.aalto.dmg.frame.bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import fi.aalto.dmg.frame.functions.FilterFunction;
import fi.aalto.dmg.frame.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jun on 02/12/15.
 */
public class IteractiveBolt<T> extends BaseBasicBolt {

    private static final Logger logger = LoggerFactory.getLogger(FilterBolt.class);
    private static final long serialVersionUID = 2401869900116259991L;
    public static String ITERATIVE_STREAM = "IterativeStream";

    FilterFunction<T> filterFunction;
    MapFunction<T, T> mapFunction;

    public IteractiveBolt(MapFunction<T, T> mapFunction, FilterFunction<T> filterFunction){
        this.mapFunction = mapFunction;
        this.filterFunction = filterFunction;
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        Object o = input.getValue(0);
        try {
            T result = this.mapFunction.map((T) o);
            if(this.filterFunction.filter(result)){
                collector.emit(ITERATIVE_STREAM, new Values(result));

            } else {
                collector.emit(new Values(result));
            }
        } catch (ClassCastException e){
            logger.error("Cast tuple[0] failed");
        } catch (Exception e) {
            e.printStackTrace();
            // logger.error("execute exception: " + e.toString());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(ITERATIVE_STREAM, new Fields(BoltConstants.OutputValueField));
        declarer.declare(new Fields(BoltConstants.OutputValueField));
    }
}