package fi.aalto.dmg.frame.userfunctions;

import com.google.common.base.Optional;
import fi.aalto.dmg.frame.functions.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by yangjun.wang on 21/10/15.
 */
public class UserFunctions {

    public static Iterable<String> split(String str){
        // TODO: trim()
        return Arrays.asList(str.split("\\W+"));
    }

    public static <T extends Number> Double sum(T t1, T t2){
        return t1.doubleValue() + t2.doubleValue();
    }

    public static MapFunction<String, String> mapToSelf = new MapFunction<String, String>(){
        public String map(String var1) {
            return var1;
        }
    };

    public static FlatMapFunction<String, String> splitFlatMap = new FlatMapFunction<String, String>() {
        public Iterable<String> flatMap(String var1) throws Exception {
            return Arrays.asList(var1.toLowerCase().split("\\W+"));
        }
    };

    public static  MapPairFunction<String, String, Integer> mapToStringIntegerPair = new MapPairFunction<String, String, Integer>() {
        public Tuple2<String, Integer> mapPair(String s) {
            return new Tuple2<String, Integer>(s, 1);
        }
    };

    public static ReduceFunction<Integer> sumReduce = new ReduceFunction<Integer>() {
        public Integer reduce(Integer var1, Integer var2) throws Exception {
            return var1 + var2;
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
            for(Tuple2<String, Integer> tuple2 : tuple2s){
                String word = tuple2._1();
                Tuple2<String, Integer> count = map.get(word);
                if (count == null){
                    map.put(word, tuple2);
                } else {
                    map.put(word, new Tuple2<>(word, count._2() + tuple2._2()));
                }
            }
            return map.values();
        }
    };
}
