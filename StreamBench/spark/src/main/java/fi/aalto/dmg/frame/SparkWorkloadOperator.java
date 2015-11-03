package fi.aalto.dmg.frame;

import fi.aalto.dmg.frame.functions.*;
import fi.aalto.dmg.frame.functions.FlatMapFunction;
import fi.aalto.dmg.util.TimeDurations;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
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
    public <R> WorkloadOperator<R> map(final MapFunction<T, R> fun, String componentId) {
        JavaDStream<R> newStream = dStream.map(new FunctionImpl<>(fun));
        return new SparkWorkloadOperator<R>(newStream);
    }

    @Override
    public <K, V> PairedWorkloadOperator<K, V> mapToPair(final MapPairFunction<T, K, V> fun, String componentId) {
        JavaPairDStream<K,V> pairDStream = dStream.mapToPair(new PairFunctionImpl<>(fun));
        return new SparkPairedWorkloadOperator<>(pairDStream);
    }

    @Override
    public WorkloadOperator<T> reduce(final ReduceFunction<T> fun, String componentId) {
        JavaDStream<T> newStream = dStream.reduce(new ReduceFunctionImpl<>(fun));
        return new SparkWorkloadOperator<>(newStream);
    }

    @Override
    public WorkloadOperator<T> filter(final FilterFunction<T> fun, String componentId) {
        JavaDStream<T> newStream = dStream.filter(new FilterFunctionImpl<>(fun));
        return new SparkWorkloadOperator<>(newStream);
    }

    @Override
    public <R> WorkloadOperator<R> flatMap(final FlatMapFunction<T, R> fun, String componentId) {
        JavaDStream<R> newStream = dStream.flatMap(new FlatMapFunctionImpl<>(fun));
        return new SparkWorkloadOperator<>(newStream);
    }

    @Override
    public WindowedWordloadOperator<T> window(TimeDurations windowDuration) {
        return window(windowDuration, windowDuration);
    }

    @Override
    public WindowedWordloadOperator<T> window(TimeDurations windowDuration, TimeDurations slideDuration) {
        Duration windowDurations = Durations.seconds(1);
        switch(windowDuration.getUnit()){
            case MILLISECONDS:
                windowDurations = Durations.milliseconds(windowDuration.getLength());
                break;
            case SECONDS:
                windowDurations = Durations.seconds(windowDuration.getLength());
                break;
            case MINUTES:
                windowDurations = Durations.minutes(windowDuration.getLength());
                break;
        }
        Duration slideDurations = Durations.seconds(1);
        switch(slideDuration.getUnit()){
            case MILLISECONDS:
                slideDurations = Durations.milliseconds(slideDuration.getLength());
                break;
            case SECONDS:
                slideDurations = Durations.seconds(slideDuration.getLength());
                break;
            case MINUTES:
                slideDurations = Durations.minutes(slideDuration.getLength());
                break;
        }
        JavaDStream<T> windowedStream = dStream.window(windowDurations, slideDurations);
        return new SparkWindowedWorkloadOperator<>(windowedStream);
    }

    @Override
    public void print() {
        this.dStream.print();
    }

}
