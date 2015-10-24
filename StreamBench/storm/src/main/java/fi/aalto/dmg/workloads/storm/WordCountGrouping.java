package fi.aalto.dmg.workloads.storm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import fi.aalto.dmg.exceptions.WorkloadException;
import fi.aalto.dmg.workloads.WordCountWorkload;
import storm.kafka.*;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Created by yangjun.wang on 20/10/15.
 */
public class WordCountGrouping extends WordCountWorkload {
    public WordCountGrouping() throws WorkloadException {
        super();
    }

    @Override
    public void Process() throws WorkloadException {
        TopologyBuilder builder = new TopologyBuilder();
        BrokerHosts hosts = new ZkHosts(this.kafkaSource.getKafkaZookeeperConnect());
        SpoutConfig spoutConfig = new SpoutConfig(hosts, this.getTopic(), "/" + this.getTopic(), UUID.randomUUID().toString());
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConfig.forceFromStart = true;

        builder.setSpout("spout", new KafkaSpout(spoutConfig), this.getSourceNum());
        builder.setBolt("split", new SplitSentence(), this.getSpliterNum()).shuffleGrouping("spout");
        builder.setBolt("counter", new WordCountBolt(), this.getCounterNum()).fieldsGrouping("split", new Fields("word"));

        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(3);

        try {
            StormSubmitter.submitTopologyWithProgressBar("WordCount", conf, builder.createTopology());
        } catch (AlreadyAliveException e) {
            e.printStackTrace();
        } catch (InvalidTopologyException e) {
            e.printStackTrace();
        }
    }

    public static class SplitSentence extends BaseBasicBolt {
        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector){
            String[] words = tuple.getString(0).split(" ");
            for(String word: words)
                collector.emit(new Values(word));
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

    public static class WordCountBolt extends BaseBasicBolt {
        Map<String, Integer> counts = new HashMap<String, Integer>();

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String word = tuple.getString(0);
            Integer count = counts.get(word);
            if (count == null)
                count = 0;
            count++;
            counts.put(word, count);
            collector.emit(new Values(word, count));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }
    }
}
