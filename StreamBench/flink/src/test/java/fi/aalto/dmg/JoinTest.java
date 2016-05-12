package fi.aalto.dmg;

import fi.aalto.dmg.util.TimeDurations;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import api.operators.StreamJoinOperator;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

/**
 * Created by jun on 20/11/15.
 */
public class JoinTest implements Serializable {
    private static final long serialVersionUID = -4881298165222782534L;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Window base on event time
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        KeySelector<Tuple2<String, Integer>, String> keySelector1 = new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        };

        DataStream<Tuple2<String, Integer>> gender = env
                .socketTextStream("localhost", 9999)
                .flatMap(new Splitter())
                .keyBy(keySelector1);


        KeySelector<Tuple2<String, String>, String> keySelector2 = new KeySelector<Tuple2<String, String>, String>() {
            @Override
            public String getKey(Tuple2<String, String> value) throws Exception {
                return value.f0;
            }
        };
        DataStream<Tuple2<String, String>> sex = env
                .socketTextStream("localhost", 9998)
                .flatMap(new Splitter2())
                .keyBy(keySelector2);


        TimeDurations windowDuration1 = new TimeDurations(TimeUnit.SECONDS, 30);
        TimeDurations windowDuration2 = new TimeDurations(TimeUnit.SECONDS, 30);

        JoinFunction<Tuple2<String, Integer>, Tuple2<String, String>, Tuple3<String, String, Integer>> joinFunction
                = new JoinFunction<Tuple2<String, Integer>, Tuple2<String, String>, Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3<String, String, Integer> join(Tuple2<String, Integer> first, Tuple2<String, String> second) throws Exception {
                return new Tuple3<>(first.f0, second.f1, first.f1);
            }
        };
        StreamJoinOperator<String, Tuple2<String, Integer>, Tuple2<String, String>, Tuple3<String, String, Integer>> joinOperator
                = new StreamJoinOperator<>(
                joinFunction,
                keySelector1,
                keySelector2,
                windowDuration1.toMilliSeconds(),
                windowDuration2.toMilliSeconds(),
                gender.getType().createSerializer(env.getConfig()),
                sex.getType().createSerializer(env.getConfig())
        );


        TypeInformation<Tuple3<String, String, Integer>> resultType = TypeExtractor.getBinaryOperatorReturnType(
                joinOperator.getUserFunction(),
                JoinFunction.class,
                true,
                true,
                gender.getType(),
                sex.getType(),
                "Join",
                false);

        TwoInputTransformation<Tuple2<String, Integer>, Tuple2<String, String>, Tuple3<String, String, Integer>>
                twoInputTransformation = new TwoInputTransformation<>(gender.getTransformation(), sex.getTransformation(), "Join", joinOperator, resultType, 2);

        DataStream<Tuple3<String, String, Integer>> joinedNewStream = new DataStream<>(env, twoInputTransformation);
        joinedNewStream.print();


//        WindowAssigner<Object, TimeWindow> windowAssigner = SlidingTimeWindows.of(Time.of(9, TimeUnit.SECONDS), Time.of(3, TimeUnit.SECONDS));
//        WindowAssigner<Object, TimeWindow> windowAssigner2 = TumblingTimeWindows.of(Time.of(3, TimeUnit.SECONDS));
//        MultiWindowsJoinedStreams<Tuple2<String, Integer>, Tuple2<String, String>> joinedStreams2 = new MultiWindowsJoinedStreams<>(gender, sex);
//        DataStream<Tuple3<String, String, Integer>> joinedNewStream2 = joinedStreams2
//                .where(keySelector1)
//                .window(windowAssigner)
//                .equalTo(keySelector2)
//                .window(windowAssigner2)
//                .apply(new JoinFunction<Tuple2<String,Integer>, Tuple2<String,String>, Tuple3<String, String, Integer>>() {
//                    @Override
//                    public Tuple3<String, String, Integer> join(Tuple2<String, Integer> first, Tuple2<String, String> second) throws Exception {
//                        return new Tuple3<>(first.f0, second.f1, first.f1);
//                    }
//                });
//        joinedNewStream2.print();

        env.execute("Socket Stream WordCount");
    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String name : sentence.split(" ")) {
                out.collect(new Tuple2<String, Integer>(name, 1));
            }
        }
    }

    public static class Splitter2 implements FlatMapFunction<String, Tuple2<String, String>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, String>> out) throws Exception {
            for (String name : sentence.split(" ")) {
                out.collect(new Tuple2<String, String>(name, "M"));
            }
        }
    }

}
