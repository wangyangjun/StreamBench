package fi.aalto.dmg.frame.functions;

import fi.aalto.dmg.Utils;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.Iterator;

/**
 * Created by jun on 11/4/15.
 */
public class PairMapPartitionFunctionImpl<K,V,R> implements PairFlatMapFunction<Iterator<Tuple2<K, V>>, K, R> {
    MapPartitionFunction<Tuple2<K, V>, Tuple2<K, R>> fun;

    public PairMapPartitionFunctionImpl(MapPartitionFunction<Tuple2<K, V>, Tuple2<K, R>> function){
        this.fun = function;
    }

    @Override
    public Iterable<Tuple2<K, R>> call(Iterator<Tuple2<K, V>> tuple2Iterator) throws Exception {
        return fun.mapPartition(Utils.iterable(tuple2Iterator));
    }
}
