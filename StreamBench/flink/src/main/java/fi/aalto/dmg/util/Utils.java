package fi.aalto.dmg.util;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;

/**
 * Created by jun on 18/11/15.
 */
public class Utils {

    public static <K, V> Tuple2<K, V> toFlinkTuple2(scala.Tuple2<K, V> tuple2) {
        return new Tuple2<>(tuple2._1(), tuple2._2());
    }

    public static <K, V> scala.Tuple2<K, V> toScalaTuple2(Tuple2<K, V> tuple2) {
        return new scala.Tuple2<>(tuple2.f0, tuple2.f1);
    }

    public static <K, V> Iterable<Tuple2> toFlinkTuple2s(Iterable<scala.Tuple2<K, V>> tuple2s) {
        ArrayList<Tuple2> flinkTuple2s = new ArrayList<>();
        for (scala.Tuple2 tuple2 : tuple2s) {
            flinkTuple2s.add(new Tuple2<>(tuple2._1(), tuple2._2()));
        }
        return flinkTuple2s;
    }

    public static <K, V> Iterable<scala.Tuple2<K, V>> toScalaTuple2s(Iterable<Tuple2<K, V>> values) {
        ArrayList<scala.Tuple2<K, V>> scalaTuple2s = new ArrayList<>();
        for (Tuple2<K, V> tuple2 : values) {
            scalaTuple2s.add(new scala.Tuple2<>(tuple2.f0, tuple2.f1));
        }
        return scalaTuple2s;
    }
}
