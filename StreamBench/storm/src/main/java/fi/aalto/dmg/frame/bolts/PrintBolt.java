package fi.aalto.dmg.frame.bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by yangjun.wang on 02/11/15.
 */
public class PrintBolt<T> extends BaseBasicBolt {

    private static final Logger logger = LoggerFactory.getLogger(PrintBolt.class);
    private ArrayList<T> collect;
    public PrintBolt(){
        collect = new ArrayList<>();
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        logger.error(input.getValue(0).toString());
        collect.add((T) input.getValue(0));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void cleanup() {
        for(T t : collect){
            logger.error(t.toString());
        }
    }

}
