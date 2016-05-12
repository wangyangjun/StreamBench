package fi.aalto.dmg.frame.userfunctions;

import com.google.common.base.Optional;
import fi.aalto.dmg.frame.functions.*;
import fi.aalto.dmg.statistics.CentroidLog;
import fi.aalto.dmg.statistics.LatencyLog;
import fi.aalto.dmg.statistics.ThroughputLog;
import fi.aalto.dmg.util.Configure;
import fi.aalto.dmg.util.Point;
import fi.aalto.dmg.util.WithTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.*;

/**
 * Created by yangjun.wang on 21/10/15.
 * User defined functions used in workloads
 */
public class UserFunctions {

    public static Iterable<String> split(String str) {
        // TODO: trim()
        return Arrays.asList(str.split("\\W+"));
    }

    public static <T extends Number> Double sum(T t1, T t2) {
        return t1.doubleValue() + t2.doubleValue();
    }

    public static MapFunction<String, String> mapToSelf = new MapFunction<String, String>() {
        public String map(String var1) {
            return var1;
        }
    };

    /**
     * Split string to string list
     */
    public static FlatMapFunction<String, String> splitFlatMap
            = new FlatMapFunction<String, String>() {
        public Iterable<String> flatMap(String var1) throws Exception {
            return Arrays.asList(var1.toLowerCase().split("\\W+"));
        }
    };

    /**
     * Map String str to pair (str, 1)
     */
    public static MapPairFunction<String, String, Integer> mapToStringIntegerPair = new MapPairFunction<String, String, Integer>() {
        public Tuple2<String, Integer> mapToPair(String s) {
            return new Tuple2<String, Integer>(s, 1);
        }
    };

    /**
     * Reduce function: return sum of two integers
     */
    public static ReduceFunction<Integer> sumReduce = new ReduceFunction<Integer>() {
        public Integer reduce(Integer var1, Integer var2) throws Exception {
            return var1 + var2;
        }
    };

    public static FlatMapFunction<WithTime<String>, WithTime<String>> splitFlatMapWithTime
            = new FlatMapFunction<WithTime<String>, WithTime<String>>() {
        public Iterable<WithTime<String>> flatMap(WithTime<String> var1) throws Exception {
            List<WithTime<String>> list = new ArrayList<>();
            for (String str : var1.getValue().toLowerCase().split("\\W+")) {
                list.add(new WithTime<>(str, var1.getTime()));
            }
            return list;
        }
    };

    public static MapPairFunction<WithTime<String>, String, WithTime<Integer>> mapToStrIntPairWithTime
            = new MapPairFunction<WithTime<String>, String, WithTime<Integer>>() {
        public Tuple2<String, WithTime<Integer>> mapToPair(WithTime<String> s) {
            return new Tuple2<>(s.getValue(), new WithTime<>(1, s.getTime()));
        }
    };

    public static ReduceFunction<WithTime<Integer>> sumReduceWithTime = new ReduceFunction<WithTime<Integer>>() {

        ThroughputLog throughput = new ThroughputLog("WordCountReduce");

        public WithTime<Integer> reduce(WithTime<Integer> var1, WithTime<Integer> var2) throws Exception {
            throughput.execute();
            return new WithTime<>(var1.getValue() + var2.getValue(), Math.max(var1.getTime(), var2.getTime()));
        }
    };

    public static ReduceFunction<WithTime<Integer>> sumReduceWithTime2 = new ReduceFunction<WithTime<Integer>>() {
        public WithTime<Integer> reduce(WithTime<Integer> var1, WithTime<Integer> var2) throws Exception {
            return new WithTime<>(var1.getValue() + var2.getValue(), Math.max(var1.getTime(), var2.getTime()));
        }
    };

    public static UpdateStateFunction<Integer> updateStateCount = new UpdateStateFunction<Integer>() {
        public Optional<Integer> update(List<Integer> values, Optional<Integer> cumulateValue) {
            Integer sum = cumulateValue.or(0);
            for (Integer i : values) {
                sum += i;
            }
            return Optional.of(sum);
        }
    };

    public static MapPartitionFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> localCount = new MapPartitionFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
        @Override
        public Iterable<Tuple2<String, Integer>> mapPartition(Iterable<Tuple2<String, Integer>> tuple2s) {
            Map<String, Tuple2<String, Integer>> map = new HashMap<>();
            for (Tuple2<String, Integer> tuple2 : tuple2s) {
                String word = tuple2._1();
                Tuple2<String, Integer> count = map.get(word);
                if (count == null) {
                    map.put(word, tuple2);
                } else {
                    map.put(word, new Tuple2<>(word, count._2() + tuple2._2()));
                }
            }
            return map.values();
        }
    };

    public static MapFunction<WithTime<Integer>, Integer> removeTimeMap = new MapFunction<WithTime<Integer>, Integer>() {
        private Logger logger = LoggerFactory.getLogger(this.getClass());

        @Override
        public Integer map(WithTime<Integer> var1) {
            logger.warn(var1.toString());
            return var1.getValue();
        }
    };

    public static FlatMapPairFunction<WithTime<String>, String, WithTime<Integer>> flatMapToPairWithTime
            = new FlatMapPairFunction<WithTime<String>, String, WithTime<Integer>>() {
        @Override
        public Iterable<Tuple2<String, WithTime<Integer>>> flatMapToPair(WithTime<String> var1) throws Exception {
            List<Tuple2<String, WithTime<Integer>>> results = new ArrayList<>();
            for (String str : var1.getValue().toLowerCase().split("\\W+")) {
                results.add(new Tuple2<>(str, new WithTime<>(1, var1.getTime())));
            }
            return results;
        }
    };

    public static FlatMapPairFunction<String, String, WithTime<Integer>> flatMapToPairAddTime
            = new FlatMapPairFunction<String, String, WithTime<Integer>>() {
        @Override
        public Iterable<Tuple2<String, WithTime<Integer>>> flatMapToPair(String var1) throws Exception {
            List<Tuple2<String, WithTime<Integer>>> results = new ArrayList<>();
            for (String str : var1.toLowerCase().split("\\W+")) {
                results.add(new Tuple2<>(str, new WithTime<>(1, System.currentTimeMillis())));
            }
            return results;
        }
    };

    /**
     * Map String "str  long" to pair (str, long)
     */
    public static MapPairFunction<String, String, Long> mapToStringLongPair = new MapPairFunction<String, String, Long>() {
        @Override
        public Tuple2<String, Long> mapToPair(String s) {
            String[] list = s.split("\\t");
            if (2 == list.length) {
                try {
                    long timestamp = Long.parseLong(list[0]);
                    String advId = list[1];
                    return new Tuple2<>(advId, timestamp);
                } catch (NumberFormatException ex) {
                    return null;
                }
            }
            return null;
        }
    };

    public static MapFunction<Tuple2<Long, Long>, WithTime<Tuple2<Long, Long>>> mapToWithTime
            = new MapFunction<Tuple2<Long, Long>, WithTime<Tuple2<Long, Long>>>() {
        ThroughputLog throughput = new ThroughputLog("MapToWithTime");

        @Override
        public WithTime<Tuple2<Long, Long>> map(Tuple2<Long, Long> var1) {
            throughput.execute();
            return new WithTime<>(var1, var1._2());
        }
    };


    public static MapFunction<WithTime<String>, WithTime<Point>>
            extractPoint = new MapFunction<WithTime<String>, WithTime<Point>>() {
        @Override
        public WithTime<Point> map(WithTime<String> var1) {
            String[] strs = var1.getValue().split("\t");
            double[] location = new double[strs.length];
            for (int i = 0; i < strs.length; i++) {
                location[i] = Double.parseDouble(strs[i]);
            }
            return new WithTime<>(new Point(location), var1.getTime());
        }
    };

    public static MapWithInitListFunction<Point, Point> assign
            = new MapWithInitListFunction<Point, Point>() {

        LatencyLog latency = new LatencyLog("CentroidAssign");
//        Logger logger = LoggerFactory.getLogger("LatestCentroids");

        @Override
        public Point map(Point var1, List<Point> list) {

            if (var1.isCentroid()) {
                // log latency
                latency.execute(var1.getTime());
                // log list for test
//                if(Math.random()<0.01) {
//                    logger.warn(list.toString());
//                }
                list.set(var1.id, var1);
                return null;
            } else {
                int minIndex = -1;
                double minDistance = Double.MAX_VALUE;

                for (int i = 0; i < list.size(); i++) {
                    double distance = var1.euclideanDistance(list.get(i));
                    if (distance < minDistance) {
                        minDistance = distance;
                        minIndex = i;
                    }
                }
                return new Point(minIndex, var1.location, var1.getTime());
            }
        }
    };

    public static MapPairFunction<Point, Integer, Tuple2<Long, Point>> pointMapToPair
            = new MapPairFunction<Point, Integer, Tuple2<Long, Point>>() {
        @Override
        public Tuple2<Integer, Tuple2<Long, Point>> mapToPair(Point point) {
            return new Tuple2<>(point.id, new Tuple2<>(1L, point));
        }
    };

    public static ReduceFunction<Tuple2<Long, Point>> pointAggregator =
            new ReduceFunction<Tuple2<Long, Point>>() {
                @Override
                public Tuple2<Long, Point> reduce(Tuple2<Long, Point> var1, Tuple2<Long, Point> var2) throws Exception {
                    double[] location = new double[var1._2.dimension()];
                    for (int i = 0; i < location.length; i++) {
                        location[i] = var1._2.location[i] + var2._2.location[i];
                    }
                    long time = Math.max(var1._2.getTime(), var2._2.getTime());
                    return new Tuple2<>(var1._1 + var2._1, new Point(location, time));
                }
            };


    public static MapFunction<Tuple2<Integer, Tuple2<Long, Point>>, Point> computeCentroid
            = new MapFunction<Tuple2<Integer, Tuple2<Long, Point>>, Point>() {
        Logger logger = LoggerFactory.getLogger("CentroidLogger");
        ThroughputLog throughput = new ThroughputLog("Centroid");
        CentroidLog centroidLog = new CentroidLog();

        @Override
        public Point map(Tuple2<Integer, Tuple2<Long, Point>> var1) {
            throughput.execute();
            long counts = var1._2()._1();

            double[] location = new double[var1._2._2.dimension()];
            for (int i = 0; i < location.length; i++) {
                location[i] = var1._2._2.location[i] / counts;
            }

            centroidLog.execute(counts, location);
            return new Point(var1._1, location, var1._2._2.getTime());
        }
    };

    public static MapWithInitListFunction<Point, Double> centroidConverge
            = new MapWithInitListFunction<Point, Double>() {

        @Override
        public Double map(Point point, List<Point> list) {

            return list.get(point.id).euclideanDistance(point);
        }
    };
}
