package fi.aalto.dmg.frame;

import fi.aalto.dmg.frame.functions.*;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;

/**
 * Created by yangjun.wang on 24/10/15.
 */
public class SparkPairedWorkloadOperator<K,V> extends SparkWorkloadOperator<Tuple2<K,V>> implements PairedWorkloadOperator<K,V> {

    private JavaPairDStream<K,V> pairDStream;

    public SparkPairedWorkloadOperator(JavaPairDStream<K, V> stream){
        super(stream.toJavaDStream());
        this.pairDStream = stream;
    }

    @Override
    public SparkGroupedWorkloadOperator<K, V> groupByKey() {
        JavaPairDStream<K, Iterable<V>> newStream = pairDStream.groupByKey();
        return new SparkGroupedWorkloadOperator<>(newStream);
    }

    @Override
    public PairedWorkloadOperator<K, V> reduceByKey(final ReduceFunction<V> fun, String componentId) {
        JavaPairDStream<K,V> newStream = pairDStream.reduceByKey(new ReduceFunctionImpl<>(fun));
        return new SparkPairedWorkloadOperator<>(newStream);
    }

    @Override
    public PairedWorkloadOperator<K, V> updateStateByKey(final UpdateStateFunction<V> fun, String componentId) {
        JavaPairDStream<K, V> cumulateStream = pairDStream.updateStateByKey(new UpdateStateFunctionImpl<>(fun));
        return new SparkPairedWorkloadOperator<>(cumulateStream);
    }


}

