package fi.aalto.dmg.frame;

import fi.aalto.dmg.frame.functions.GrouperPairFunctionImpl;
import fi.aalto.dmg.frame.functions.ReduceFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;

/**
 * Created by yangjun.wang on 25/10/15.
 */
public class SparkWorkloadGrouperOperator<K,V> implements WorkloadGrouperOperator<K,V> {

    private JavaPairDStream<K, Iterable<V>> pairDStream;

    public SparkWorkloadGrouperOperator(JavaPairDStream<K, Iterable<V>> stream){
        this.pairDStream = stream;
    }

    @Override
    public SparkWorkloadPairOperator<K, V> reduce(final ReduceFunction<V> fun) {
        JavaPairDStream<K,V> newStream = this.pairDStream.mapToPair(new GrouperPairFunctionImpl(fun));
        return new SparkWorkloadPairOperator<>(newStream);
    }
}
