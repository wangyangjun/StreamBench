package fi.aalto.dmg.frame;

import com.google.common.base.Optional;
import fi.aalto.dmg.frame.functions.*;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;

import java.util.List;

/**
 * Created by yangjun.wang on 24/10/15.
 */
public class SparkWorkloadPairOperator<K,V> extends SparkWorkloadOperator<Tuple2<K,V>> implements WorkloadPairOperator<K,V> {

    private JavaPairDStream<K,V> pairDStream;

    public SparkWorkloadPairOperator(JavaPairDStream<K,V> stream){
        super(stream.toJavaDStream());
        this.pairDStream = stream;
}

    @Override
    public SparkWorkloadGrouperOperator<K, V> groupByKey() {
        JavaPairDStream<K, Iterable<V>> newStream = pairDStream.groupByKey();
        return new SparkWorkloadGrouperOperator<>(newStream);
    }

    @Override
    public WorkloadPairOperator<K, V> reduceByKey(K key, final ReduceFunction<V> fun) {
        JavaPairDStream<K,V> newStream = pairDStream.reduceByKey(new Function2<V, V, V>() {
            @Override
            public V call(V v, V v2) throws Exception {
                return fun.reduce(v, v2);
            }
        });
        return new SparkWorkloadPairOperator<>(newStream);
    }

    @Override
    public WorkloadPairOperator<K, V> updateStateByKey(final UpdateStateFunction<V> fun) {
        JavaPairDStream<K, V> cumulateStream = pairDStream.updateStateByKey(new UpdateStateFunctionImpl(fun));
        return new SparkWorkloadPairOperator<>(cumulateStream);
    }


}

