package fi.aalto.dmg;


import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
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
 * Created by jun on 11/11/15.
 */

public class TickTest {

    public static final int DEFAULT_TICK_FREQUENCY_SECONDS = 4;

    public static void main(String[] args) throws WorkloadException {
        TopologyBuilder builder = new TopologyBuilder();
        BrokerHosts hosts = new ZkHosts("localhost:2181");
        SpoutConfig spoutConfig = new SpoutConfig(hosts, "WordCount", "/" + "WordCount", UUID.randomUUID().toString());
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConfig.ignoreZkOffsets = true;

        builder.setSpout("spout", new KafkaSpout(spoutConfig));
        builder.setBolt("split", new SplitSentence()).shuffleGrouping("spout");
        builder.setBolt("counter", new CounterBolt(), 3).shuffleGrouping("split");
        builder.setBolt("aggregator", new AggregatorBolt(), 1)
                .fieldsGrouping("counter", Utils.DEFAULT_STREAM_ID, new Fields("word"))
                .allGrouping("counter", "tick");

        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(3);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("kafka-spout", conf, builder.createTopology());
    }


    public static class SplitSentence extends BaseBasicBolt {
        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String[] words = tuple.getString(0).split(" ");
            for (String word : words) {
                if (null != word && !word.isEmpty()) {
                    collector.emit(new Values(word.trim().toLowerCase()));
                }
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }

    public static class CounterBolt extends BaseBasicBolt {
        private Map<String, Integer> counts = new HashMap<String, Integer>();


        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            if (isTickTuple(tuple)) {
                System.out.print("Tick tuple");
                System.out.println(System.currentTimeMillis());
                for (Map.Entry<String, Integer> entry : counts.entrySet()) {
                    String word = entry.getKey();
                    Integer count = entry.getValue();
                    collector.emit(Utils.DEFAULT_STREAM_ID, new Values(word, count));
                }
                collector.emit("tick", new Values());
                counts.clear();
                return;
            }

            String word = tuple.getString(0);
            Integer count = counts.get(word);
            if (count == null)
                count = 0;
            count++;
            counts.put(word, count);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declareStream(Utils.DEFAULT_STREAM_ID, new Fields("word", "count"));
            declarer.declareStream("tick", new Fields());
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            Config conf = new Config();
            conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, DEFAULT_TICK_FREQUENCY_SECONDS);
            return conf;
        }

        private static boolean isTickTuple(Tuple tuple) {
            return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                    && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
        }
    }

    public static class AggregatorBolt extends BaseBasicBolt {
        Map<String, Integer> counts = new HashMap<String, Integer>();

        @Override
        public void prepare(Map stormConf, TopologyContext context) {
            Map<GlobalStreamId, Grouping> map = context.getThisSources();
            for (Map.Entry<GlobalStreamId, Grouping> entry : map.entrySet()) {
                if (entry.getKey().get_streamId().equals(Utils.DEFAULT_STREAM_ID)) {
                    List<Integer> list = context.getComponentTasks(entry.getKey().get_componentId());
                    System.out.println(list.toString());
                }
            }
        }

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            if (isTickTuple(tuple)) {
                System.out.print("Tick tuple!");
                System.out.println(System.currentTimeMillis());
            } else {
                String word = tuple.getString(0);
                Integer delta_count = tuple.getInteger(1);
                Integer count = counts.get(word);
                if (count == null)
                    count = 0;
                count = count + delta_count;
                counts.put(word, count);
                collector.emit(new Values(word, count));
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }


        private static boolean isTickTuple(Tuple tuple) {
            return (tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                    && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID))
                    || (tuple.getSourceStreamId().equals("tick"));
        }
    }
}