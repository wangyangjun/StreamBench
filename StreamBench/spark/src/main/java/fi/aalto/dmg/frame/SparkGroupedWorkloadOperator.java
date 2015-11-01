package fi.aalto.dmg.frame;

import fi.aalto.dmg.frame.functions.GrouperPairFunctionImpl;
import fi.aalto.dmg.frame.functions.ReduceFunction;
import org.apache.spark.streaming.api.java.JavaPairDStream;

/**
 * Created by yangjun.wang on 25/10/15.
 */
public class SparkGroupedWorkloadOperator<K,V> implements GroupedWorkloadOperator<K,V> {

    private JavaPairDStream<K, Iterable<V>> pairDStream;

    public SparkGroupedWorkloadOperator(JavaPairDStream<K, Iterable<V>> stream){
        this.pairDStream = stream;
    }

    @Override
    public SparkPairedWorkloadOperator<K, V> reduce(final ReduceFunction<V> fun, String componentId) {
        JavaPairDStream<K,V> newStream = this.pairDStream.mapToPair(new GrouperPairFunctionImpl<K,V>(fun));
        return new SparkPairedWorkloadOperator<>(newStream);
    }
}
