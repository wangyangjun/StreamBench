package fi.aalto.dmg.frame;

import backtype.storm.LocalCluster;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;
import java.util.UUID;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import fi.aalto.dmg.frame.bolts.ExtractPointBolt;
import fi.aalto.dmg.frame.bolts.WithTimeBolt;
import fi.aalto.dmg.util.Point;
import fi.aalto.dmg.util.WithTime;
import storm.kafka.*;

/**
 * Created by yangjun.wang on 01/11/15.
 */
public class StormOperatorCreator extends OperatorCreator implements Serializable {

    private static final long serialVersionUID = 4498355837057651696L;
    private Properties properties;
    private Config conf;
    private TopologyBuilder topologyBuilder;

    public StormOperatorCreator(String name) throws IOException {
        super(name);
        properties = new Properties();
        properties.load(this.getClass().getClassLoader().getResourceAsStream("storm-cluster.properties"));

        conf = new Config();
        conf.setDebug(true);

        // ack enabled
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 100);

        // ack disable
//        conf.put(Config.TOPOLOGY_ACKER_EXECUTORS, 0);
        conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 60);
        topologyBuilder = new TopologyBuilder();
    }

    @Override
    public WorkloadOperator<WithTime<String>> stringStreamFromKafkaWithTime(String zkConStr,
                                                                            String kafkaServers,
                                                                            String group,
                                                                            String topics,
                                                                            String offset,
                                                                            String componentId,
                                                                            int parallelism) {
        conf.setNumWorkers(parallelism);
        BrokerHosts hosts = new ZkHosts(zkConStr);
        SpoutConfig spoutConfig = new SpoutConfig(hosts, topics, "/" + topics, UUID.randomUUID().toString());
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        if( offset.endsWith("smallest")) {
            spoutConfig.startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
        }
        spoutConfig.fetchSizeBytes = 1024;
        spoutConfig.bufferSizeBytes = 1024;
//        spoutConfig.ignoreZkOffsets = true;

        topologyBuilder.setSpout("spout", new KafkaSpout(spoutConfig), parallelism);
        topologyBuilder.setBolt("addTime", new WithTimeBolt<String>(), parallelism).localOrShuffleGrouping("spout");
        return new StormOperator<>(topologyBuilder, "addTime", parallelism);
    }

    @Override
    public WorkloadOperator<Point> pointStreamFromKafka(String zkConStr, String kafkaServers, String group, String topics, String offset, String componentId, int parallelism) {
        conf.setNumWorkers(parallelism);
        BrokerHosts hosts = new ZkHosts(zkConStr);
        SpoutConfig spoutConfig = new SpoutConfig(hosts, topics, "/" + topics, UUID.randomUUID().toString());
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        if( offset.endsWith("smallest")) {
            spoutConfig.startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
        }
        spoutConfig.fetchSizeBytes = 1024;
        spoutConfig.bufferSizeBytes = 1024;
//        spoutConfig.ignoreZkOffsets = true;

        topologyBuilder.setSpout("spout", new KafkaSpout(spoutConfig), parallelism);
        topologyBuilder.setBolt("extractPoint", new ExtractPointBolt(), parallelism).localOrShuffleGrouping("spout");
        return new StormOperator<>(topologyBuilder, "extractPoint", parallelism);
    }

    @Override
    public WorkloadOperator<String> stringStreamFromKafka(String zkConStr,
                                                          String kafkaServers,
                                                          String group,
                                                          String topics,
                                                          String offset,
                                                          String componentId,
                                                          int parallelism) {
        conf.setNumWorkers(parallelism);
        BrokerHosts hosts = new ZkHosts(zkConStr);
        SpoutConfig spoutConfig = new SpoutConfig(hosts, topics, "/" + topics, UUID.randomUUID().toString());
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        spoutConfig.fetchSizeBytes = 1024;
        spoutConfig.bufferSizeBytes = 1024;
//        spoutConfig.ignoreZkOffsets = true;

        topologyBuilder.setSpout(componentId, new KafkaSpout(spoutConfig), parallelism);
        return new StormOperator<>(topologyBuilder, componentId, parallelism);
    }

    @Override
    public void Start() {
        // TODO: switch between local and cluster
        try {
            StormSubmitter.submitTopologyWithProgressBar(this.getAppName(), conf, topologyBuilder.createTopology());
        } catch (AlreadyAliveException e) {
            e.printStackTrace();
        } catch (InvalidTopologyException e) {
            e.printStackTrace();
        } catch (AuthorizationException e) {
            e.printStackTrace();
        }

//        LocalCluster cluster = new LocalCluster();
//        cluster.submitTopology("word-count", conf, topologyBuilder.createTopology());
    }
}
