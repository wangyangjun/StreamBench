package fi.aalto.dmg.frame;

import fi.aalto.dmg.exceptions.UnsupportOperatorException;
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
public class SparkPairWorkloadOperator<K,V> extends PairWorkloadOperator<K,V> {

    private static final long serialVersionUID = 7879350341179747221L;
    private JavaPairDStream<K,V> pairDStream;

    public SparkPairWorkloadOperator(JavaPairDStream<K, V> stream, int parallelism){
        super(parallelism);
        this.pairDStream = stream;
    }

    @Override
    public SparkGroupedWorkloadOperator<K, V> groupByKey() {
        JavaPairDStream<K, Iterable<V>> newStream = pairDStream.groupByKey();
        return new SparkGroupedWorkloadOperator<>(newStream, parallelism);
    }

    @Override
    public PairWorkloadOperator<K, V> reduceByKey(ReduceFunction<V> fun, String componentId, boolean logThroughput) {
        return reduceByKey(fun, componentId);
    }

    @Override
    public PairWorkloadOperator<K, V> reduceByKey(final ReduceFunction<V> fun, String componentId) {
        JavaPairDStream<K,V> newStream = pairDStream.reduceByKey(new ReduceFunctionImpl<>(fun));
        return new SparkPairWorkloadOperator<>(newStream, parallelism);
    }

    @Override
    public <R> PairWorkloadOperator<K, R> mapValue(MapFunction<V, R> fun, String componentId, boolean logThroughput) {
        return mapValue(fun, componentId);
    }

    @Override
    public <R> PairWorkloadOperator<K, R> mapValue(MapFunction<V, R> fun, String componentId) {
        JavaPairDStream<K,R> newStream = pairDStream.mapValues(new FunctionImpl<>(fun));
        return new SparkPairWorkloadOperator<>(newStream, parallelism);
    }

    @Override
    public <R> WorkloadOperator<R> map(MapFunction<Tuple2<K, V>, R> fun, String componentId, boolean logThroughput) {
        return null;
    }

    @Override
    public <R> WorkloadOperator<R> map(MapFunction<Tuple2<K, V>, R> fun, String componentId) {
        return null;
    }

    @Override
    public <R> WorkloadOperator<R> map(MapFunction<Tuple2<K, V>, R> fun, String componentId, Class<R> outputClass) {
        return null;
    }

    @Override
    public <R> WorkloadOperator<R> map(MapFunction<Tuple2<K, V>, R> fun, String componentId, Class<R> outputClass, boolean logThroughput) {
        return null;
    }

    @Override
    public <R> PairWorkloadOperator<K, R> flatMapValue(FlatMapFunction<V, R> fun, String componentId, boolean logThroughput) {
        return flatMapValue(fun, componentId);
    }

    @Override
    public <R> PairWorkloadOperator<K, R> flatMapValue(FlatMapFunction<V, R> fun, String componentId) {
        JavaPairDStream<K,R> newStream = pairDStream.flatMapValues(new FlatMapValuesFunctionImpl<>(fun));
        return new SparkPairWorkloadOperator<>(newStream, parallelism);
    }

    @Override
    public PairWorkloadOperator<K, V> filter(FilterFunction<Tuple2<K, V>> fun, String componentId,
                                             boolean logThroughput) {
        return filter(fun, componentId);
    }

    @Override
    public PairWorkloadOperator<K, V> filter(FilterFunction<Tuple2<K, V>> fun, String componentId) {
        JavaPairDStream<K,V> newStream = pairDStream.filter(new FilterFunctionImpl<>(fun));
        return new SparkPairWorkloadOperator<>(newStream, parallelism);
    }

    @Override
    public PairWorkloadOperator<K, V> updateStateByKey(ReduceFunction<V> fun, String componentId, boolean logThroughput) {
        return updateStateByKey(fun, componentId);
    }

    @Override
    public PairWorkloadOperator<K, V> updateStateByKey(final ReduceFunction<V> fun, String componentId) {
        JavaPairDStream<K, V> cumulateStream = pairDStream.updateStateByKey(new UpdateStateFunctionImpl<>(fun));
//        cumulateStream.checkpoint(new Duration(60000));
        return new SparkPairWorkloadOperator<>(cumulateStream, parallelism);
    }

    @Override
    public PairWorkloadOperator<K, V> reduceByKeyAndWindow(ReduceFunction<V> fun, String componentId,
                                                           TimeDurations windowDuration, boolean logThroughput) {
        return reduceByKeyAndWindow(fun, componentId, windowDuration);
    }

    @Override
    public PairWorkloadOperator<K, V> reduceByKeyAndWindow(ReduceFunction<V> fun, String componentId,
                                                           TimeDurations windowDuration) {
        return reduceByKeyAndWindow(fun, componentId, windowDuration, windowDuration);
    }

    @Override
    public PairWorkloadOperator<K, V> reduceByKeyAndWindow(ReduceFunction<V> fun, String componentId,
                                                           TimeDurations windowDuration, TimeDurations slideDuration, boolean logThroughput) {
        return reduceByKeyAndWindow(fun, componentId, windowDuration, slideDuration);
    }

    @Override
    public PairWorkloadOperator<K, V> reduceByKeyAndWindow(ReduceFunction<V> fun, String componentId,
                                                           TimeDurations windowDuration, TimeDurations slideDuration) {
        Duration windowDurations = Utils.timeDurationsToSparkDuration(windowDuration);
        Duration slideDurations = Utils.timeDurationsToSparkDuration(slideDuration);
        JavaPairDStream<K, V> accumulateStream = pairDStream.reduceByKeyAndWindow(new ReduceFunctionImpl<V>(fun), windowDurations, slideDurations);
        return new SparkPairWorkloadOperator<>(accumulateStream, parallelism);
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
        return new SparkWindowedPairWorkloadOperator<>(windowedStream, parallelism);
    }

    /**
     * Join two streams base on processing time
     * @param componentId
     * @param joinStream
     *          the other stream<K,R>
     * @param windowDuration
     *          window length of this stream
     * @param joinWindowDuration
     *          window length of joinStream
     * @param <R>
     * @return
     * @throws WorkloadException
     */
    @Override
    public <R> PairWorkloadOperator<K, Tuple2<V, R>> join(String componentId,
                                                          PairWorkloadOperator<K, R> joinStream,
                                                          TimeDurations windowDuration,
                                                          TimeDurations joinWindowDuration) throws WorkloadException {
        if(windowDuration.toMilliSeconds()%windowDuration.toMilliSeconds()!=0){
            throw new WorkloadException("WindowDuration should be multi times of joinWindowDuration");
        }
        Duration windowDurations = Utils.timeDurationsToSparkDuration(windowDuration);
        Duration windowDurations2 = Utils.timeDurationsToSparkDuration(joinWindowDuration);

        if(joinStream instanceof SparkPairWorkloadOperator) {
            SparkPairWorkloadOperator<K, R> joinSparkStream = ((SparkPairWorkloadOperator<K, R>) joinStream);
            JavaPairDStream<K, Tuple2<V, R>> joinedStream = pairDStream
                    .window(windowDurations.plus(windowDurations2), windowDurations2)
                    .join(joinSparkStream.pairDStream.window(windowDurations2, windowDurations2));
            // filter illegal joined data

            return new SparkPairWorkloadOperator<>(joinedStream, parallelism);
        }
        throw new WorkloadException("Cast joinStream to SparkPairWorkloadOperator failed");
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
                                                          final AssignTimeFunction<V> eventTimeAssigner1,
                                                          final AssignTimeFunction<R> eventTimeAssigner2) throws WorkloadException {
        if(windowDuration.toMilliSeconds()%windowDuration.toMilliSeconds()!=0){
            throw new WorkloadException("WindowDuration should be multi times of joinWindowDuration");
        }
        final Duration windowDurations = Utils.timeDurationsToSparkDuration(windowDuration);
        Duration windowDurations2 = Utils.timeDurationsToSparkDuration(joinWindowDuration);

        if(joinStream instanceof SparkPairWorkloadOperator) {
            SparkPairWorkloadOperator<K, R> joinSparkStream = ((SparkPairWorkloadOperator<K, R>) joinStream);
            JavaPairDStream<K, Tuple2<V, R>> joinedStream = pairDStream
                    .window(windowDurations.plus(windowDurations2), windowDurations2)
                    .join(joinSparkStream.pairDStream.window(windowDurations2, windowDurations2));
            // filter illegal joined data
//            joinedStream.filter(filterFun);

            return new SparkPairWorkloadOperator<>(joinedStream, parallelism);
        }
        throw new WorkloadException("Cast joinStream to SparkPairWorkloadOperator failed");
    }

    @Override
    public void closeWith(OperatorBase stream, boolean broadcast) throws UnsupportOperatorException {
        throw new UnsupportOperatorException("not implemented yet");
    }

    @Override
    public void print() {
        this.pairDStream.print();
    }

    @Override
    public void sink() {
        this.pairDStream = this.pairDStream.filter(new PairLatencySinkFunction<K,V>());
        this.pairDStream.count().print();
    }
}

