package fi.aalto.dmg.frame.functions;

import java.util.Iterator;

import fi.aalto.dmg.util.Utils;
import org.apache.spark.api.java.function.FlatMapFunction;

/**
 * Created by yangjun.wang on 28/10/15.
 */
public class MapPartitionFunctionImpl<T,R> implements FlatMapFunction<Iterator<T>, R> {

    private static final long serialVersionUID = 1330526225779542078L;
    private MapPartitionFunction<T, R> fun;

    public MapPartitionFunctionImpl(MapPartitionFunction<T, R> function){
        fun = function;
    }

    @Override
    public Iterable<R> call(Iterator<T> tIterator) throws Exception {
        return fun.mapPartition(Utils.iterable(tIterator));
    }
}
