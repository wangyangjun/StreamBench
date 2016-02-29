package fi.aalto.dmg.frame.bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import fi.aalto.dmg.util.Constant;
import fi.aalto.dmg.util.Point;

/**
 * Created by jun on 01/03/16.
 */
public class ExtractPointBolt extends BaseBasicBolt {

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String str = (String)input.getValue(0);
        String[] list = str.split(Constant.TimeSeparatorRegex);
        long time = System.currentTimeMillis();
        if(list.length == 2) {
            time = Long.parseLong(list[1]);
        }
        String[] strs = list[0].split("\t");
        Double x = Double.parseDouble(strs[0]);
        Double y = Double.parseDouble(strs[1]);
        collector.emit(new Values(new Point(x, y, time)));

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(BoltConstants.OutputValueField));

    }
}
