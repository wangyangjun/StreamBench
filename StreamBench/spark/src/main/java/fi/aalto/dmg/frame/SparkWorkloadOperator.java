package fi.aalto.dmg.frame;

import fi.aalto.dmg.frame.functions.*;
import fi.aalto.dmg.frame.functions.FlatMapFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;


/**
 * Created by yangjun.wang on 23/10/15.
 */
public class SparkWorkloadOperator<T> extends OperatorBase implements WorkloadOperator<T> {
    private JavaDStream<T> dStream;

    public SparkWorkloadOperator(JavaDStream<T> stream){
        dStream = stream;
    }


    @Override
    public <R> WorkloadOperator<R> map(final MapFunction<T, R> fun) {
        JavaDStream<R> newStream = dStream.map(new FunctionImpl<>(fun));
        return new SparkWorkloadOperator<R>(newStream);
    }

    @Override
    public <R> WorkloadOperator<R> mapPartition(final MapPartitionFunction<T, R> fun) {
        JavaDStream<R> newStream = dStream.mapPartitions(new MapPartitionFunctionImpl<>(fun));
        return new SparkWorkloadOperator<R>(newStream);
    }

    @Override
    public <K, V> WorkloadPairOperator<K, V> mapToPair(final MapPairFunction<T, K, V> fun) {
        JavaPairDStream<K,V> pairDStream = dStream.mapToPair(new PairFunctionImpl<>(fun));
        return new SparkWorkloadPairOperator<>(pairDStream);
    }

    @Override
    public WorkloadOperator<T> reduce(final ReduceFunction<T> fun) {
        JavaDStream<T> newStream = dStream.reduce(new ReduceFunctionImpl<>(fun));
        return new SparkWorkloadOperator<T>(newStream);
    }

    @Override
    public WorkloadOperator<T> filter(final FilterFunction<T> fun) {
        JavaDStream<T> newStream = dStream.filter(new FilterFunctionImpl<>(fun));
        return new SparkWorkloadOperator<T>(newStream);
    }

    @Override
    public <R> WorkloadOperator<R> flatMap(final FlatMapFunction<T, R> fun) {
        JavaDStream<R> newStream = dStream.flatMap(new FlatMapFunctionImpl<>(fun));
        return new SparkWorkloadOperator<R>(newStream);
    }

    @Override
    public void print() {
        this.dStream.print();
    }

}
