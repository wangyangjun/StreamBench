package fi.aalto.dmg;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import scala.Tuple2;

import java.io.Serializable;

/**
 * Created by jun on 20/11/15.
 */
public class InvalidTypesExceptionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> counts = env
                .socketTextStream("localhost", 9999)
                .flatMap(new Splitter());

        DataStream<Tuple2<String, Integer>> pairs = mapToPair(counts, mapToStringIntegerPair);
        pairs.print();
        env.execute("Socket Stream WordCount");
    }

    public static class Splitter implements FlatMapFunction<String, String> {
        @Override
        public void flatMap(String sentence, Collector<String> out) throws Exception {
            for (String word: sentence.split(" ")) {
                out.collect(word);
            }
        }
    }

    public static  <K,V,T> DataStream<Tuple2<K,V>> mapToPair(DataStream<T> dataStream , final MapPairFunction<T, K, V> fun){
        return dataStream.map(new MapFunction<T, Tuple2<K, V>>() {
            @Override
            public Tuple2<K, V> map(T t) throws Exception {
                return fun.mapPair(t);
            }
        });
    }

    public interface MapPairFunction<T, K, V> extends Serializable {
        Tuple2<K,V> mapPair(T t);
    }

    public static MapPairFunction<String, String, Integer> mapToStringIntegerPair = new MapPairFunction<String, String, Integer>() {
        public Tuple2<String, Integer> mapPair(String s) {
            return new Tuple2<String, Integer>(s, 1);
        }
    };
}
