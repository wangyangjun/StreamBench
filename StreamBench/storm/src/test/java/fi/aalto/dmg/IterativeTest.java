package fi.aalto.dmg;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.LocalCluster;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import fi.aalto.dmg.exceptions.WorkloadException;
import storm.kafka.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Created by jun on 23/11/15.
 */
public class IterativeTest {

    public static final int DEFAULT_TICK_FREQUENCY_SECONDS = 4;

    public static void main(String[] args) throws WorkloadException {
        TopologyBuilder builder = new TopologyBuilder();


        builder.setSpout("spout", new NumberSpout());
        builder.setBolt("minusone", new MinusOne())
                .shuffleGrouping("spout")
                .shuffleGrouping("minusone", "GreaterThanZero");


        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(3);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("kafka-spout", conf, builder.createTopology());
    }

    public static class NumberSpout extends BaseRichSpout {
        SpoutOutputCollector _collector;
        int i;

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            _collector = collector;
            i = 0;
        }

        @Override
        public void nextTuple() {
            Utils.sleep(100);
            if( i < 10)
                _collector.emit(new Values(i++));
        }

        @Override
        public void ack(Object id) {
        }

        @Override
        public void fail(Object id) {
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

    }

    public static class MinusOne extends BaseBasicBolt {
        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            int i = tuple.getInteger(0) - 1;
            if( i > 0) {
                System.out.println(i);
                collector.emit("GreaterThanZero", new Values(i));
            } else {
                collector.emit(new Values(i));
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declareStream("GreaterThanZero", new Fields("Value"));
            declarer.declare(new Fields("Value"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }



}