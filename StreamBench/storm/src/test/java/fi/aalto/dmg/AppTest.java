package fi.aalto.dmg;


import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.metric.SystemBolt;
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
import scala.Tuple2;
import storm.kafka.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Created by jun on 11/11/15.
 */

public class AppTest  {

    public static final int DEFAULT_TICK_FREQUENCY_SECONDS = 4;

    public static void main( String[] args )  throws WorkloadException {
        TopologyBuilder builder = new TopologyBuilder();
        BrokerHosts hosts = new ZkHosts("localhost:2181");
        SpoutConfig spoutConfig = new SpoutConfig(hosts, "WordCount", "/" + "WordCount", UUID.randomUUID().toString());
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConfig.forceFromStart = true;

        builder.setSpout("spout", new KafkaSpout(spoutConfig));
        builder.setBolt("split", new SplitSentence()).shuffleGrouping("spout");
        builder.setBolt("counter", new CounterBolt(), 3).fieldsGrouping("split", new Fields("wordCountPair") );

        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(3);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("kafka-spout", conf, builder.createTopology());
    }


    public static class SplitSentence extends BaseBasicBolt {
        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector){
            String[] words = tuple.getString(0).split(" ");
            for(String word: words) {
                if( null != word && !word.isEmpty()){
                    collector.emit(new Values(new Tuple2<String,Integer>(word.trim().toLowerCase(),1)));
                }
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("wordCountPair"));
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

            for(Map.Entry<String, Integer> entry: counts.entrySet()){
                String word = entry.getKey();
                Integer count = entry.getValue();
                collector.emit(Utils.DEFAULT_STREAM_ID, new Values(word, count));
            }

            Tuple2<String, Integer> tuple2 = (Tuple2<String, Integer>) tuple.getValue(0);
            if(null != tuple2){
                Integer count = counts.get(tuple2._1());
                if (count == null)
                    count = tuple2._2();
                else
                    count += tuple2._2();
                counts.put(tuple2._1(), count);
                System.out.println(tuple2._1() + "\t" + String.valueOf(count));
            } else {
                System.out.println("Failed");
            }

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declareStream(Utils.DEFAULT_STREAM_ID, new Fields("word", "count"));
            declarer.declareStream("tick", new Fields());
        }

    }


}