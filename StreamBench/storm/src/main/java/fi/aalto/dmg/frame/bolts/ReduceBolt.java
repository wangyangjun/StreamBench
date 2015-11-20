package fi.aalto.dmg.frame.bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import fi.aalto.dmg.frame.functions.ReduceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yangjun.wang on 31/10/15.
 */
public class ReduceBolt<T> extends BaseBasicBolt {

    private static final Logger logger = LoggerFactory.getLogger(ReduceBolt.class);
    private static final long serialVersionUID = 6478439481824085537L;
    private T currentValue;

    ReduceFunction<T> fun;

    public ReduceBolt(ReduceFunction<T> function){
        this.fun = function;
        this.currentValue = null;
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        Object o = input.getValue(0);
        try {
            if(null != currentValue){
                currentValue = this.fun.reduce((T) o, currentValue);
            } else {
                currentValue = (T)o;
            }
            collector.emit(new Values(currentValue));
        } catch (ClassCastException e){
            logger.error("Cast tuple[0] failed");
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Reduce error");
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(BoltConstants.OutputValueField));
    }
}
