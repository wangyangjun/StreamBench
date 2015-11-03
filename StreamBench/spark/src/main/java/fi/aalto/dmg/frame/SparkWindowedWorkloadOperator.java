package fi.aalto.dmg.frame;

import fi.aalto.dmg.frame.WindowedWordloadOperator;
import fi.aalto.dmg.frame.WorkloadOperator;
import fi.aalto.dmg.frame.functions.*;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

/**
 * Created by yangjun.wang on 31/10/15.
 */
public class SparkWindowedWorkloadOperator<T> implements WindowedWordloadOperator<T> {
    private JavaDStream<T> dStream;

    public SparkWindowedWorkloadOperator(JavaDStream<T> stream){
        dStream = stream;
    }


    @Override
    public <R> WorkloadOperator<R> mapPartition(MapPartitionFunction<T, R> fun, String componentId) {

        JavaDStream<R> newStream = dStream.mapPartitions(new MapPartitionFunctionImpl<>(fun));
        return new SparkWorkloadOperator<>(newStream);
    }

    @Override
    public <R> WorkloadOperator<R> map(MapFunction<T, R> fun, String componentId) {
        JavaDStream<R> newStream = dStream.map(new FunctionImpl<>(fun));
        return new SparkWorkloadOperator<>(newStream);
    }

    @Override
    public WorkloadOperator<T> filter(FilterFunction<T> fun, String componentId) {
        JavaDStream<T> newStream = dStream.filter(new FilterFunctionImpl<>(fun));
        return new SparkWorkloadOperator<>(newStream);
    }

    @Override
    public WorkloadOperator<T> reduce(ReduceFunction<T> fun, String componentId) {
        JavaDStream<T> newStream = dStream.reduce(new ReduceFunctionImpl<>(fun));
        return new SparkWorkloadOperator<>(newStream);
    }

    @Override
    public <K, V> PairedWorkloadOperator<K, V> mapToPair(MapPairFunction<T, K, V> fun, String componentId) {
        JavaPairDStream<K,V> pairDStream = dStream.mapToPair(new PairFunctionImpl<>(fun));
        return new SparkPairedWorkloadOperator<>(pairDStream);
    }
}
