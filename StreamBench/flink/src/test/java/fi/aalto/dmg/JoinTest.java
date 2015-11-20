package fi.aalto.dmg;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.IntegerTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.JoinedStreams;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import scala.Tuple2;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

/**
 * Created by jun on 20/11/15.
 */
public class JoinTest implements Serializable{
    private static final long serialVersionUID = -4881298165222782534L;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> age = env
                .socketTextStream("localhost", 9999)
                .flatMap(new Splitter());

        DataStream<Tuple2<String, String>> sex = env
                .socketTextStream("localhost", 9998)
                .flatMap(new Splitter2());

        age.join(sex)
            .where(new KeySelector<Tuple2<String,Integer>, String>() {
                @Override
                public String getKey(Tuple2<String, Integer> value) throws Exception {
                    return value._1();
                }
            })
            .equalTo(new KeySelector<Tuple2<String, String>, String>() {
                @Override
                public String getKey(Tuple2<String, String> value) throws Exception {
                    return value._1();
                }
            })
            .window(TumblingTimeWindows.of(Time.of(60, TimeUnit.SECONDS)))
                .apply(new JoinFunction<Tuple2<String,Integer>, Tuple2<String,String>, Tuple3<String, String, Integer>>() {
                    @Override
                    public Tuple3<String, String, Integer> join(Tuple2<String, Integer> age, Tuple2<String, String> sex) throws Exception {
                        return new Tuple3<String, String, Integer>(sex._1(), sex._2(), age._2());
                    }
                }).print();

        env.execute("Socket Stream WordCount");
    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String name: sentence.split(" ")) {
                out.collect(new Tuple2<String, Integer>(name, 1));
            }
        }
    }

    public static class Splitter2 implements FlatMapFunction<String, Tuple2<String, String>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, String>> out) throws Exception {
            for (String name: sentence.split(" ")) {
                out.collect(new Tuple2<String, String>(name, "M"));
            }
        }
    }
}
