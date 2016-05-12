package fi.aalto.dmg;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
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
import fi.aalto.dmg.exceptions.WorkloadException;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Created by jun on 22/02/16.
 */

public class KMeansTest {

    public static final int DEFAULT_TICK_FREQUENCY_SECONDS = 4;

    public static void main(String[] args) throws WorkloadException {
        TopologyBuilder builder = new TopologyBuilder();


        builder.setSpout("points", new PointSpout());
        builder.setBolt("assign", new Assign())
                .shuffleGrouping("points")
                .allGrouping("aggregator", "centroids");

        builder.setBolt("aggregator", new Aggregator())
                .fieldsGrouping("assign", new Fields("centroid_index"));

        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(3);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("kafka-spout", conf, builder.createTopology());
    }

    public static class PointSpout extends BaseRichSpout {
        SpoutOutputCollector _collector;
        int i;

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            _collector = collector;
            i = 0;
        }

        @Override
        public void nextTuple() {
            if (i < KMeansData.POINTS.length) {
                _collector.emit(new Values(new KMeansData.Point(
                        (double) KMeansData.POINTS[i][0], (double) KMeansData.POINTS[i][1])));
                i++;
            }
        }

        @Override
        public void ack(Object id) {
        }

        @Override
        public void fail(Object id) {
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("points"));
        }

    }

    public static class Assign extends BaseBasicBolt {
        public List<KMeansData.Point> initMeanList;

        @Override
        public void prepare(Map stormConf, TopologyContext context) {
            initMeanList = new ArrayList<>();
            initMeanList.add(new KMeansData.Point(0, -31.85, -44.77));
            initMeanList.add(new KMeansData.Point(1, 35.16, 17.46));
            initMeanList.add(new KMeansData.Point(2, -5.16, 21.93));
            initMeanList.add(new KMeansData.Point(3, -24.06, 6.81));
        }

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            KMeansData.Point point = (KMeansData.Point) tuple.getValue(0);
            if (tuple.getSourceComponent().equals("points")) {
                // assign
                int minIndex = -1;
                double minDistance = Double.MAX_VALUE;

                for (int i = 0; i < initMeanList.size(); i++) {
                    double distance = point.euclideanDistance(initMeanList.get(i));
                    if (distance < minDistance) {
                        minDistance = distance;
                        minIndex = i;
                    }
                }
                point.id = minIndex;
//                System.out.println(point.toString());
                collector.emit(new Values(minIndex, point, 1L));
            } else if (tuple.getSourceComponent().equals("aggregator")) {
                // update centroids
                System.out.println(point.toString());
                initMeanList.set(point.id, point);
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("centroid_index", "assigned_points", "count"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }

    public static class Aggregator extends BaseBasicBolt {
        private Map<Integer, Tuple3<Long, Double, Double>> centroids_aggreate;

        @Override
        public void prepare(Map stormConf, TopologyContext context) {
            centroids_aggreate = new HashMap<>();
        }

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            int centroid_index = tuple.getInteger(0);
            KMeansData.Point point = (KMeansData.Point) tuple.getValue(1);
            long count = tuple.getLong(2);

            // accumulate counts, accumulate sum of x dimension, accumulate sum of y dimension
            Tuple3<Long, Double, Double> centroid_aggreate = centroids_aggreate.get(centroid_index);
            // declarer.declare(new Fields("centroid_index", "assigned_points", "count"));

            if (null == centroid_aggreate) {

                centroids_aggreate.put(centroid_index, new Tuple3<>(1L, point.x, point.y));
                collector.emit("centroids", new Values(new KMeansData.Point(centroid_index,
                        point.x,
                        point.y)));
            } else {
                long accumulate_counts = centroid_aggreate._1() + count;
                double accumulate_x = centroid_aggreate._2() + point.x;
                double accumulate_y = centroid_aggreate._3() + point.y;

                centroids_aggreate.put(centroid_index, new Tuple3<>(accumulate_counts, accumulate_x, accumulate_y));
                collector.emit("centroids", new Values(new KMeansData.Point(centroid_index,
                        accumulate_x / accumulate_counts,
                        accumulate_y / accumulate_counts)));
            }

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declareStream("centroids", new Fields("Value"));
            declarer.declare(new Fields("Value"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }

}