package fi.aalto.dmg;

import fi.aalto.dmg.frame.FlinkGroupedWorkloadOperator;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.IntegerTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.Serializable;

/**
 * Created by jun on 20/11/15.
 */
public class InvalidTypesExceptionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        DataStream<String> counts = env
//                .socketTextStream("localhost", 9999)
//                .flatMap(new Splitter());
//        DataStream<Tuple2<String, Integer>> pairs = mapToPair(counts, mapToStringIntegerPair);

        DataStream<Tuple2<String, Integer>> pairs = env
                .socketTextStream("localhost", 9999)
                .flatMap(new Splitter2());
//
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = groupByKey(pairs, String.class);
//
//        keyedStream.print();
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

    public static class Splitter2 implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String name: sentence.split(" ")) {
                out.collect(new Tuple2<String, Integer>(name, 1));
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

    public static <K,V> KeyedStream<Tuple2<K,V>,K> groupByKey(DataStream<Tuple2<K,V>> dataStream, Class<K> keyClass) {
        KeySelector<Tuple2<K, V>, K> keySelector = new KeySelector<Tuple2<K, V>, K>() {
            @Override
            public K getKey(Tuple2<K, V> value) throws Exception {
                return value.f0;
            }
        };
        KeySelector<K,K> keySelector2 = new KeySelector<K, K>() {
            @Override
            public K getKey(K value) throws Exception {
                return value;
            }
        };
        // get key type
//        BasicTypeInfo.STRING_TYPE_INFO
//        TypeInformation<K> keyTypeInfo = TypeExtractor.getKeySelectorTypes(keySelector, dataStream.getType());
        TypeInformation<K> keyTypeInfo = TypeExtractor.getUnaryOperatorReturnType(keySelector2, KeySelector.class, false, false, dataStream.getType(), null ,false);
        KeyedStream<Tuple2<K, V>, K> keyedStream = new KeyedStream<>(dataStream, keySelector, keyTypeInfo);
        return keyedStream;
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
