package fi.aalto.dmg.frame;

import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;
import java.util.UUID;

import backtype.storm.Config;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.*;

/**
 * Created by yangjun.wang on 01/11/15.
 */
public class StormOperatorCreater extends OperatorCreater implements Serializable {

    private Properties properties;
    private Config conf;
    private TopologyBuilder topologyBuilder;

    public StormOperatorCreater(String name) throws IOException {
        super(name);
        properties = new Properties();
        properties.load(this.getClass().getClassLoader().getResourceAsStream("storm-cluster.properties"));

        conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(3);

        topologyBuilder = new TopologyBuilder();
    }

    @Override
    public WorkloadOperator<String> createOperatorFromKafka(String zkConStr, String kafkaServers, String group, String topics, String offset) {
        BrokerHosts hosts = new ZkHosts(zkConStr);
        SpoutConfig spoutConfig = new SpoutConfig(hosts, topics, "/" + topics, UUID.randomUUID().toString());
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConfig.forceFromStart = true;

        topologyBuilder.setSpout("spout", new KafkaSpout(spoutConfig));
        return new StormWorkloadOperator<>(topologyBuilder, "spout");
    }

    @Override
    public void Start() {
        // StormSubmitter.submitTopologyWithProgressBar("WordCount", conf, topologyBuilder.createTopology());

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("word-count", conf, topologyBuilder.createTopology());
    }
}
