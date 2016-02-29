package fi.aalto.dmg.frame;

import fi.aalto.dmg.frame.functions.*;
import fi.aalto.dmg.frame.functions.FlatMapFunction;
import fi.aalto.dmg.util.TimeDurations;
import fi.aalto.dmg.util.Utils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;


/**
 * Created by yangjun.wang on 23/10/15.
 */
public class SparkWorkloadOperator<T> extends WorkloadOperator<T> {
    private static final long serialVersionUID = 1265982206392632383L;
    private JavaDStream<T> dStream;

    public SparkWorkloadOperator(JavaDStream<T> stream, int parallelism){
        super(parallelism);
        dStream = stream;
    }

    @Override
    public <R> WorkloadOperator<R> map(MapFunction<T, R> fun,
                                       String componentId,
                                       boolean logThroughput) {
        return map(fun, componentId);
    }

    @Override
    public <R> WorkloadOperator<R> map(final MapFunction<T, R> fun,
                                       String componentId) {
        JavaDStream<R> newStream = dStream.map(new FunctionImpl<>(fun));
        return new SparkWorkloadOperator<R>(newStream, parallelism);
    }

    @Override
    public <K, V> PairWorkloadOperator<K, V> mapToPair(MapPairFunction<T, K, V> fun, String componentId, boolean logThroughput) {
        return mapToPair(fun, componentId);
    }

    @Override
    public <K, V> PairWorkloadOperator<K, V> mapToPair(final MapPairFunction<T, K, V> fun, String componentId) {
        JavaPairDStream<K,V> pairDStream = dStream.mapToPair(new PairFunctionImpl<>(fun));
        return new SparkPairWorkloadOperator<>(pairDStream, parallelism);
    }

    @Override
    public WorkloadOperator<T> reduce(ReduceFunction<T> fun,
                                      String componentId,
                                      boolean logThroughput) {
        return reduce(fun, componentId);
    }

    @Override
    public WorkloadOperator<T> reduce(final ReduceFunction<T> fun, String componentId) {
        JavaDStream<T> newStream = dStream.reduce(new ReduceFunctionImpl<>(fun));
        return new SparkWorkloadOperator<>(newStream, parallelism);
    }

    @Override
    public WorkloadOperator<T> filter(FilterFunction<T> fun, String componentId, boolean logThroughput) {
        return filter(fun, componentId);
    }

    @Override
    public WorkloadOperator<T> filter(final FilterFunction<T> fun, String componentId) {
        JavaDStream<T> newStream = dStream.filter(new FilterFunctionImpl<>(fun));
        return new SparkWorkloadOperator<>(newStream, parallelism);
    }

    @Override
    public <R> WorkloadOperator<R> flatMap(FlatMapFunction<T, R> fun,
                                           String componentId,
                                           boolean logThroughput) {
        return flatMap(fun, componentId);
    }

    @Override
    public <R> WorkloadOperator<R> flatMap(final FlatMapFunction<T, R> fun,
                                           String componentId) {
        JavaDStream<R> newStream = dStream.flatMap(new FlatMapFunctionImpl<>(fun));
        return new SparkWorkloadOperator<>(newStream, parallelism);
    }


    @Override
    public <K, V> PairWorkloadOperator<K, V> flatMapToPair(final FlatMapPairFunction<T, K, V> fun,
                                                           String componentId,
                                                           boolean logThroughput) {
        return flatMapToPair(fun, componentId);
    }

    @Override
    public <K, V> PairWorkloadOperator<K, V> flatMapToPair(final FlatMapPairFunction<T, K, V> fun,
                                                           String componentId) {
        JavaPairDStream<K,V> pairDStream = dStream.flatMapToPair(new PairFlatMapFunction<T, K, V>() {
            @Override
            public Iterable<Tuple2<K, V>> call(T t) throws Exception {
                return fun.flatMapToPair(t);
            }
        });
        return new SparkPairWorkloadOperator<>(pairDStream, parallelism);
    }

    @Override
    public WindowedWorkloadOperator<T> window(TimeDurations windowDuration) {
        return window(windowDuration, windowDuration);
    }

    @Override
    public WindowedWorkloadOperator<T> window(TimeDurations windowDuration,
                                              TimeDurations slideDuration) {
        Duration windowDurations = Utils.timeDurationsToSparkDuration(windowDuration);
        Duration slideDurations = Utils.timeDurationsToSparkDuration(slideDuration);

        JavaDStream<T> windowedStream = dStream.window(windowDurations, slideDurations);
        return new SparkWindowedWorkloadOperator<>(windowedStream, parallelism);
    }

    @Override
    public void sink() {

    }

    @Override
    public void print() {

    }
}
