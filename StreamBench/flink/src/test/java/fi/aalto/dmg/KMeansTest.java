package fi.aalto.dmg;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by jun on 19/02/16.
 */
public class KMeansTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        // <Integer, Integer, Long, Long> ~
        // (origin 1 or feedback 0, cluster index, accumulated record number, record value)
        DataStream<Tuple4<Integer, Integer, Long, Long>> someIntegers = env.generateSequence(0, 10)
                .map(new MapFunction<Long, Tuple4<Integer, Integer, Long, Long>>() {
                    @Override
                    public Tuple4<Integer, Integer, Long, Long> map(Long value) throws Exception {
                        return new Tuple4<>(1, -1, 1L, value);
                    }
                });

        class Assign implements MapFunction<Tuple4<Integer, Integer, Long, Long>, Tuple4<Integer, Integer, Long, Long>> {
            public List<Long> initMeanList;

            public Assign() {
                initMeanList = new ArrayList<>();
                initMeanList.add((long) new Random().nextInt(10));
                initMeanList.add((long) new Random().nextInt(10));
                initMeanList.add((long) new Random().nextInt(10));
                System.out.println(initMeanList);
            }

            @Override
            public Tuple4<Integer, Integer, Long, Long> map(Tuple4<Integer, Integer, Long, Long> value) throws Exception {
                System.out.println(initMeanList.toString() + String.valueOf(System.identityHashCode(initMeanList)));
                // Update mean
                if (value.f0 == 0) {
                    initMeanList.set(value.f1, value.f3);
                    return value;
                } else {
                    int minIndex = -1;
                    long minDistance = Long.MAX_VALUE;

                    for (int i = 0; i < initMeanList.size(); i++) {
                        long distance = Math.abs(value.f3 - initMeanList.get(i));
                        if (distance < minDistance) {
                            minDistance = distance;
                            minIndex = i;
                        }
                    }
                    return new Tuple4<>(1, minIndex, 1L, value.f3);
                }
            }
        }

        IterativeStream<Tuple4<Integer, Integer, Long, Long>> iteration = someIntegers.iterate();

        DataStream<Tuple4<Integer, Integer, Long, Long>> assigned = iteration.map(new Assign());

        DataStream<Tuple4<Integer, Integer, Long, Long>> updateMeans = assigned.filter(new FilterFunction<Tuple4<Integer, Integer, Long, Long>>() {
            @Override
            public boolean filter(Tuple4<Integer, Integer, Long, Long> value) throws Exception {
                return value.f0 == 1;
            }
        }).keyBy(1).reduce(new ReduceFunction<Tuple4<Integer, Integer, Long, Long>>() {
            @Override
            public Tuple4<Integer, Integer, Long, Long> reduce(Tuple4<Integer, Integer, Long, Long> value1, Tuple4<Integer, Integer, Long, Long> value2) throws Exception {
                long totalElementNum = value1.f2 + value2.f2;
                long mean = (value1.f2 * value1.f3 + value2.f2 * value2.f3) / totalElementNum;
                return new Tuple4<>(0, value1.f1, totalElementNum, mean);
            }
        }).broadcast();

        iteration.closeWith(updateMeans);
//        iteration.print();
        updateMeans.print();
//        assigned.print();

        env.execute("Iterative test");

    }

}