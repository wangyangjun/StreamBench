package fi.aalto.dmg;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

import fi.aalto.dmg.KMeansData.Point;
import operators.PointAssignMap;

/**
 * Created by jun on 21/02/16.
 */
public class KMeansTest3 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        // <Integer, Integer, Long, Long> ~
        // ( cluster index, accumulated record number, record value)
        DataStream<Tuple2<Long, Point>> someIntegers = KMeansData.getDefaultPointDataStream(env)
                .map(new MapFunction<Point, Tuple2<Long, Point>>() {
                    @Override
                    public Tuple2<Long, Point> map(Point value) throws Exception {
                        return new Tuple2<>(1L, value);
                    }
                });

        class Assign implements MapFunction<Tuple2<Long, Point>, Tuple2<Long, Point>> {
            public List<Point> initMeanList;

            public Assign() {
                initMeanList = new ArrayList<>();
                initMeanList.add(new Point(0, -31.85, -44.77));
                initMeanList.add(new Point(1, 35.16, 17.46));
                initMeanList.add(new Point(2, -5.16, 21.93));
                initMeanList.add(new Point(3, -24.06, 6.81));
//                System.out.println(initMeanList.toString() + String.valueOf(System.identityHashCode(initMeanList)));

            }

            @Override
            public Tuple2<Long, Point> map(Tuple2<Long, Point> value) throws Exception {
//                System.out.println(initMeanList.toString() + String.valueOf(System.identityHashCode(initMeanList)));
                // Update mean
                if (value.f1.isCentroid()) {
                    initMeanList.set(value.f1.id, value.f1);
                    return null;
                } else {
                    int minIndex = -1;
                    double minDistance = Double.MAX_VALUE;

                    for (int i = 0; i < initMeanList.size(); i++) {
                        double distance = value.f1.euclideanDistance(initMeanList.get(i));
                        if (distance < minDistance) {
                            minDistance = distance;
                            minIndex = i;
                        }
                    }
                    value.f1.id = minIndex;
                    return new Tuple2<>(1L, value.f1);
                }
            }
        }

        IterativeStream<Tuple2<Long, Point>> iteration = someIntegers.iterate();

//        DataStream<Tuple2<Long, Point>> assigned = iteration.map(new Assign());
        Assign assign = new Assign();
//
        TypeInformation<Tuple2<Long, Point>> outType = TypeExtractor.getMapReturnTypes(env.clean(assign), someIntegers.getType(),
                Utils.getCallLocationName(), true);
        DataStream<Tuple2<Long, Point>> assigned =
                iteration.transform("Map", outType, new PointAssignMap<>(env.clean(assign)));


        DataStream<Tuple2<Long, Point>> updateMeans
                = assigned
//                .filter(new FilterFunction<Tuple2<Long, Point>>() {
//                    @Override
//                    public boolean filter(Tuple2<Long, Point> value) throws Exception {
//                        if(value.f0 > 1) {
//                            return false;
//                        }
//                        return true;
//                    }
//                })
                .keyBy(new KeySelector<Tuple2<Long, Point>, Integer>() {
                    @Override
                    public Integer getKey(Tuple2<Long, Point> longPointTuple2) throws Exception {
                        return longPointTuple2.f1.id;
                    }
                })
                .reduce(new ReduceFunction<Tuple2<Long, Point>>() {
                    @Override
                    public Tuple2<Long, Point> reduce(Tuple2<Long, Point> value1,
                                                      Tuple2<Long, Point> value2) throws Exception {
                        long totalElementNum = value1.f0 + value2.f0;
                        Point mean = value1.f1.mul(value1.f0).add(value2.f1.mul(value2.f0)).div(totalElementNum);
                        return new Tuple2<>(totalElementNum, mean);
                    }
                }).broadcast();

        iteration.closeWith(updateMeans);
//        iteration.print();
//        updateMeans.print();
        assigned.print();

        env.execute("Iterative test");

    }

}