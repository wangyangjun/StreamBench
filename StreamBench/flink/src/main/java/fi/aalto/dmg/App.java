package fi.aalto.dmg;

import fi.aalto.dmg.exceptions.WorkloadException;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.regex.Pattern;

/**
 * Hello world!
 *
 */
public class App
{
    private static final Pattern SPACE = Pattern.compile(" ");
    private static Logger logger = LoggerFactory.getLogger(App.class);
    public static void main( String[] args ) throws ClassNotFoundException, WorkloadException, InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException, IOException {

        logger.warn("Start...");
        String[] testArgs = {"WordCount"}; // WordCount WordCountWindowed FasterWordCount, ClickedAdvertisement
        BenchStarter.main(testArgs);

    }

    /*
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> counts = env
                .socketTextStream("master", 9999)
                .flatMap(new Splitter())
                .keyBy(new KeySelector<Tuple2<String,Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                })
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
                    }
                });
//        counts.flatMap(new FlatMapFunction<Tuple2<String,Integer>, Object>() {
//            @Override
//            public void flatMap(Tuple2<String, Integer> value, Collector<Object> out) throws Exception {
//                System.out.println(value);
//            }
//        });

        counts.addSink(new SinkFunction<Tuple2<String, Integer>>() {
            @Override
            public void invoke(Tuple2<String, Integer> value) throws Exception {
                System.out.println(value.toString());
            }
        });

        env.execute("Socket Stream WordCount");
    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word: sentence.split(" ")) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }
    */
}
