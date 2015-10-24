package fi.aalto.dmg.frame;

import fi.aalto.dmg.frame.functions.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;

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
        JavaDStream<R> newStream = dStream.map(new Function<T, R>() {
            @Override
            public R call(T t) throws Exception {
                return fun.map(t);
            }
        });
        return new SparkWorkloadOperator<R>(newStream);
    }

    @Override
    public <K, V> WorkloadPairOperator<K, V> mapToPair(final MapPairFunction<T, K, V> fun) {
        JavaPairDStream<K,V> pairDStream = dStream.mapToPair(new PairFunction<T, K, V>() {
            @Override
            public Tuple2<K, V> call(T t) throws Exception {
                return fun.map(t);
            }
        });
        return new SparkWorkloadPairOperator(pairDStream);
    }

    @Override
    public WorkloadOperator<T> reduce(final ReduceFunction<T> fun) {
        JavaDStream<T> newStream = dStream.reduce(new Function2<T, T, T>() {
            @Override
            public T call(T t, T t2) throws Exception {
                return fun.reduce(t, t2);
            }
        });
        return new SparkWorkloadOperator<T>(newStream);
    }

    @Override
    public WorkloadOperator<T> filter(final FilterFunction<T> fun) {
        JavaDStream<T> newStream = dStream.filter(new Function<T, Boolean>() {
            @Override
            public Boolean call(T t) throws Exception {
                return fun.filter(t);
            }
        });
        return new SparkWorkloadOperator<T>(newStream);
    }

    @Override
    public <R> WorkloadOperator<R> flatMap(final FlatMapFunction<T, R> fun) {
        JavaDStream<R> newStream = dStream.flatMap(new org.apache.spark.api.java.function.FlatMapFunction<T, R>() {
            @Override
            public java.lang.Iterable<R> call(T t) throws Exception {
                return fun.flatMap(t);
            }
        });
        return new SparkWorkloadOperator<R>(newStream);
    }

}
