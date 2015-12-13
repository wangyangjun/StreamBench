package fi.aalto.dmg.frame;

import fi.aalto.dmg.exceptions.WorkloadException;
import fi.aalto.dmg.frame.functions.*;
import fi.aalto.dmg.util.TimeDurations;
import fi.aalto.dmg.util.Utils;
import fi.aalto.dmg.util.WithTime;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 * Created by yangjun.wang on 24/10/15.
 */
public class SparkPairWorkloadOperator<K,V> implements PairWorkloadOperator<K,V> {

    private static final long serialVersionUID = 7879350341179747221L;
    private JavaPairDStream<K,V> pairDStream;

    public SparkPairWorkloadOperator(JavaPairDStream<K, V> stream){
        this.pairDStream = stream;
    }

    @Override
    public SparkGroupedWorkloadOperator<K, V> groupByKey() {
        JavaPairDStream<K, Iterable<V>> newStream = pairDStream.groupByKey();
        return new SparkGroupedWorkloadOperator<>(newStream);
    }

    @Override
    public PairWorkloadOperator<K, V> reduceByKey(ReduceFunction<V> fun, String componentId, boolean logThroughput) {
        return reduceByKey(fun, componentId);
    }

    @Override
    public PairWorkloadOperator<K, V> reduceByKey(final ReduceFunction<V> fun, String componentId) {
        JavaPairDStream<K,V> newStream = pairDStream.reduceByKey(new ReduceFunctionImpl<>(fun));
        return new SparkPairWorkloadOperator<>(newStream);
    }

    @Override
    public <R> PairWorkloadOperator<K, R> mapValue(MapFunction<V, R> fun, String componentId, boolean logThroughput) {
        return mapValue(fun, componentId);
    }

    @Override
    public <R> PairWorkloadOperator<K, R> mapValue(MapFunction<V, R> fun, String componentId) {
        JavaPairDStream<K,R> newStream = pairDStream.mapValues(new FunctionImpl<>(fun));
        return new SparkPairWorkloadOperator<>(newStream);
    }

    @Override
    public <R> PairWorkloadOperator<K, R> flatMapValue(FlatMapFunction<V, R> fun, String componentId, boolean logThroughput) {
        return flatMapValue(fun, componentId);
    }

    @Override
    public <R> PairWorkloadOperator<K, R> flatMapValue(FlatMapFunction<V, R> fun, String componentId) {
        JavaPairDStream<K,R> newStream = pairDStream.flatMapValues(new FlatMapValuesFunctionImpl<>(fun));
        return new SparkPairWorkloadOperator<>(newStream);
    }

    @Override
    public PairWorkloadOperator<K, V> filter(FilterFunction<Tuple2<K, V>> fun, String componentId, boolean logThroughput) {
        return filter(fun, componentId);
    }

    @Override
    public PairWorkloadOperator<K, V> filter(FilterFunction<Tuple2<K, V>> fun, String componentId) {
        JavaPairDStream<K,V> newStream = pairDStream.filter(new FilterFunctionImpl<>(fun));
        return new SparkPairWorkloadOperator<>(newStream);
    }

    @Override
    public PairWorkloadOperator<K, V> iterative(MapFunction<V, V> mapFunction, FilterFunction<Tuple2<K, V>> iterativeFunction, String componentId) {
        return null;
    }

    @Override
    public PairWorkloadOperator<K, V> updateStateByKey(ReduceFunction<V> fun, String componentId, boolean logThroughput) {
        return updateStateByKey(fun, componentId);
    }

    @Override
    public PairWorkloadOperator<K, V> updateStateByKey(final ReduceFunction<V> fun, String componentId) {
        JavaPairDStream<K, V> cumulateStream = pairDStream.updateStateByKey(new UpdateStateFunctionImpl<>(fun));
        return new SparkPairWorkloadOperator<>(cumulateStream);
    }

    @Override
    public PairWorkloadOperator<K, V> reduceByKeyAndWindow(ReduceFunction<V> fun, String componentId, TimeDurations windowDuration, boolean logThroughput) {
        return reduceByKeyAndWindow(fun, componentId, windowDuration);
    }

    @Override
    public PairWorkloadOperator<K, V> reduceByKeyAndWindow(ReduceFunction<V> fun, String componentId, TimeDurations windowDuration) {
        return reduceByKeyAndWindow(fun, componentId, windowDuration, windowDuration);
    }

    @Override
    public PairWorkloadOperator<K, V> reduceByKeyAndWindow(ReduceFunction<V> fun, String componentId, TimeDurations windowDuration, TimeDurations slideDuration, boolean logThroughput) {
        return reduceByKeyAndWindow(fun, componentId, windowDuration, slideDuration);
    }

    @Override
    public PairWorkloadOperator<K, V> reduceByKeyAndWindow(ReduceFunction<V> fun, String componentId, TimeDurations windowDuration, TimeDurations slideDuration) {
        Duration windowDurations = Utils.timeDurationsToSparkDuration(windowDuration);
        Duration slideDurations = Utils.timeDurationsToSparkDuration(slideDuration);
        JavaPairDStream<K, V> accumulateStream = pairDStream.reduceByKeyAndWindow(new ReduceFunctionImpl<V>(fun), windowDurations, slideDurations);
        return new SparkPairWorkloadOperator<>(accumulateStream);
    }


    @Override
    public WindowedPairWorkloadOperator<K,V> window(TimeDurations windowDuration) {
        return window(windowDuration, windowDuration);
    }

    @Override
    public WindowedPairWorkloadOperator<K, V> window(TimeDurations windowDuration, TimeDurations slideDuration) {
        Duration windowDurations = Utils.timeDurationsToSparkDuration(windowDuration);
        Duration slideDurations = Utils.timeDurationsToSparkDuration(slideDuration);
        JavaPairDStream<K, V> windowedStream = pairDStream.window(windowDurations, slideDurations);
        return new SparkWindowedPairWorkloadOperator<>(windowedStream);
    }

    /**
     *
     * @param componentId
     * @param stream2
     *          the other stream<K,R>
     * @param windowDuration
     *          window length of this stream
     * @param windowDuration2
     *          window length of joinStream
     * @param <R>
     * @return
     * @throws WorkloadException
     */
    @Override
    public <R> PairWorkloadOperator<K, Tuple2<V, R>> join(String componentId, PairWorkloadOperator<K, R> stream2, TimeDurations windowDuration, TimeDurations windowDuration2) throws WorkloadException {
        if(windowDuration.toMilliSeconds()%windowDuration.toMilliSeconds()!=0){
            throw new WorkloadException("WindowDuration should be multi times of joinWindowDuration");
        }
        Duration windowDurations = Utils.timeDurationsToSparkDuration(windowDuration);
        Duration windowDurations2 = Utils.timeDurationsToSparkDuration(windowDuration);

        if(stream2 instanceof SparkPairWorkloadOperator) {
            SparkPairWorkloadOperator<K, R> joinSparkStream = ((SparkPairWorkloadOperator<K, R>) stream2);
            // It is possible that illegal joined data exists
            JavaPairDStream<K, Tuple2<V, R>> joinedStream = pairDStream
                    .window(windowDurations.plus(windowDurations2), windowDurations2)
                    .join(joinSparkStream.pairDStream.window(windowDurations2));
            return new SparkPairWorkloadOperator<>(joinedStream);
        }
        throw new WorkloadException("Cast joinStrem to SparkPairWorkloadOperator failed");
    }

    /**
     * Spark doesn't support event time join yet
     * @param componentId
     * @param joinStream
     *          the other stream<K,R>
     * @param windowDuration
     *          window length of this stream
     * @param joinWindowDuration
     *          window length of joinStream
     * @param eventTimeAssigner1
     *          event time assignment for this stream
     * @param eventTimeAssigner2
     *          event time assignment for joinStream
     * @param <R>
     * @return
     * @throws WorkloadException
     */
    @Override
    public <R> PairWorkloadOperator<K, Tuple2<V, R>> join(String componentId,
                                                          PairWorkloadOperator<K, R> joinStream,
                                                          TimeDurations windowDuration,
                                                          TimeDurations joinWindowDuration,
                                                          AssignTimeFunction<V> eventTimeAssigner1,
                                                          AssignTimeFunction<R> eventTimeAssigner2) throws WorkloadException {
        return join(componentId, joinStream, windowDuration, joinWindowDuration);
    }

    @Override
    public void print() {
        this.pairDStream.print();
    }

    @Override
    public void sink() {
//        this.pairDStream = this.pairDStream.filter(new PairLatencySinkFunction<K,V>());
        this.pairDStream.print();
    }
}

