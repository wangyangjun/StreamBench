package fi.aalto.dmg;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.yarn.util.SystemClock;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import fi.aalto.dmg.KMeansData.Point;

/**
 * Created by jun on 19/02/16.
 */
public class KMeansTest2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        // <Integer, Integer, Long, Long> ~
        // ( cluster index, accumulated record number, record value)
        DataStream<Tuple3<Integer, Long, Point>> someIntegers = KMeansData.getDefaultPointDataStream(env)
                .map(new MapFunction<Point, Tuple3<Integer, Long, Point>>() {
                    @Override
                    public Tuple3<Integer, Long, Point> map(Point value) throws Exception {
                        return new Tuple3<>(-1, 1L, value);
                    }
                });

        class Assign implements MapFunction<Tuple3<Integer, Long, Point>, Tuple3<Integer, Long, Point>> {
            public List<Point> initMeanList;

            public Assign() {
                initMeanList = new ArrayList<>();
                initMeanList.add(new Point(0, -31.85, -44.77));
                initMeanList.add(new Point(1, 35.16, 17.46));
                initMeanList.add(new Point(2, -5.16, 21.93));
                initMeanList.add(new Point(3, -24.06, 6.81));
                System.out.println(initMeanList.toString() + String.valueOf(System.identityHashCode(initMeanList)));

            }

            @Override
            public Tuple3<Integer, Long, Point> map(Tuple3<Integer, Long, Point> value) throws Exception {
//                System.out.println(initMeanList.toString() + String.valueOf(System.identityHashCode(initMeanList)));
                // Update mean
                if (value.f2.isCentroid()) {
                    initMeanList.set(value.f2.id, value.f2);
                    return value;
                } else {
                    int minIndex = -1;
                    double minDistance = Double.MAX_VALUE;

                    for (int i = 0; i < initMeanList.size(); i++) {
                        double distance = value.f2.euclideanDistance(initMeanList.get(i));
                        if (distance < minDistance) {
                            minDistance = distance;
                            minIndex = i;
                        }
                    }
//                    System.out.println(String.valueOf(minIndex) + value.toString());
                    value.f2.id = minIndex;
                    return new Tuple3<>(minIndex, 1L, value.f2);
                }
            }
        }

        IterativeStream<Tuple3<Integer, Long, Point>> iteration = someIntegers.iterate();

        DataStream<Tuple3<Integer, Long, Point>> assigned = iteration.map(new Assign());

        DataStream<Tuple3<Integer, Long, Point>> updateMeans
                = assigned.filter(new FilterFunction<Tuple3<Integer, Long, Point>>() {
            @Override
            public boolean filter(Tuple3<Integer, Long, Point> value) throws Exception {
                return value.f1 <= 1;
            }
        }).keyBy(new KeySelector<Tuple3<Integer, Long, Point>, Integer>() {
            @Override
            public Integer getKey(Tuple3<Integer, Long, Point> integerLongPointTuple3) throws Exception {
                return integerLongPointTuple3.f0;
            }
        }).reduce(new ReduceFunction<Tuple3<Integer, Long, Point>>() {
            @Override
            public Tuple3<Integer, Long, Point> reduce(Tuple3<Integer, Long, Point> value1,
                                                       Tuple3<Integer, Long, Point> value2) throws Exception {
                long totalElementNum = value1.f1 + value2.f1;
                Point mean = value1.f2.mul(value1.f1).add(value2.f2.mul(value2.f1)).div(totalElementNum);
                mean.id = value1.f0;
                return new Tuple3<>(value1.f0, totalElementNum, mean);
            }
        }).broadcast();

        iteration.closeWith(updateMeans);
//        iteration.print();
//        updateMeans.print();
        assigned.print();

        env.execute("Iterative test");

    }

}