package fi.aalto.dmg.frame.bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import fi.aalto.dmg.util.Constant;
import fi.aalto.dmg.util.WithTime;

/**
 * Created by jun on 07/12/15.
 */
public class WithTimeBolt<T> extends BaseBasicBolt {

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {

        collector.emit(new Values(new WithTime<T>((T) input.getValue(0), System.currentTimeMillis())));

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(BoltConstants.OutputValueField));

    }
}
