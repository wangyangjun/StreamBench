package fi.aalto.dmg.frame;

import fi.aalto.dmg.frame.functions.*;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;

import java.util.Iterator;

/**
 * Created by jun on 11/3/15.
 */
public class SparkWindowedPairWorkloadOperator<K,V> implements WindowedPairWorkloadOperator<K,V> {

    private JavaPairDStream<K,V> pairDStream;

    public SparkWindowedPairWorkloadOperator(JavaPairDStream<K, V> stream) {
        this.pairDStream = stream;
    }

    @Override
    public WindowedPairWorkloadOperator<K, V> reduceByKey(ReduceFunction<V> fun, String componentId) {
        JavaPairDStream<K,V> newStream = this.pairDStream.reduceByKey(new ReduceFunctionImpl<>(fun));
        return new SparkWindowedPairWorkloadOperator<>(newStream);
    }

    @Override
    public PairWorkloadOperator<K, V> updateStateByKey(UpdateStateFunction<V> fun, String componentId) {
        JavaPairDStream<K, V> cumulateStream = this.pairDStream.updateStateByKey(new UpdateStateFunctionImpl<>(fun));
        return new SparkPairWorkloadOperator<>(cumulateStream);
    }

    @Override
    public <R> WindowedPairWorkloadOperator<K, R> mapPartition(MapPartitionFunction<Tuple2<K, V>, Tuple2<K, R>> fun, String componentId) {
        JavaPairDStream<K,R> newStream = pairDStream.mapPartitionsToPair(new PairMapPartitionFunctionImpl<>(fun));
        return new SparkWindowedPairWorkloadOperator<>(newStream);
    }

    @Override
    public <R> WindowedPairWorkloadOperator<K, R> mapValue(MapFunction<Tuple2<K, V>, Tuple2<K, R>> fun, String componentId) {
        return null;
    }

    @Override
    public WindowedPairWorkloadOperator<K, V> filter(FilterFunction<Tuple2<K, V>> fun, String componentId) {
        return null;
    }

    @Override
    public WindowedPairWorkloadOperator<K, V> reduce(ReduceFunction<Tuple2<K, V>> fun, String componentId) {
        return null;
    }

    @Override
    public void print() {
        pairDStream.print();
    }
}
