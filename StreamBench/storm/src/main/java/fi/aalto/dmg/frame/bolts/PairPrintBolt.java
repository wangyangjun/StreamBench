package fi.aalto.dmg.frame.bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

/**
 * Created by yangjun.wang on 02/11/15.
 */
public class PairPrintBolt<T> extends BaseBasicBolt {

    private static final Logger logger = LoggerFactory.getLogger(PairPrintBolt.class);
    public PairPrintBolt(){

    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        logger.error(input.getValue(0).toString() + "\t" + input.getValue(1).toString());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

}
