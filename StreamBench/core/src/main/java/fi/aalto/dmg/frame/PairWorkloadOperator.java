package fi.aalto.dmg.frame;

import fi.aalto.dmg.exceptions.WorkloadException;
import fi.aalto.dmg.frame.functions.*;
import fi.aalto.dmg.util.TimeDurations;
import scala.Tuple2;

import java.io.Serializable;

/**
 * Created by yangjun.wang on 24/10/15.
 */
abstract public class PairWorkloadOperator<K, V> extends OperatorBase {

    public PairWorkloadOperator(int parallelism) {
        super(parallelism);
    }

    abstract public GroupedWorkloadOperator<K, V> groupByKey();

    // TODO: translate to reduce on each node, then group merge
    abstract public PairWorkloadOperator<K, V> reduceByKey(ReduceFunction<V> fun, String componentId);

    /**
     * Map <K,V> tuple to <K,R>
     *
     * @param fun         map V to R
     * @param componentId component Id of this bolt
     * @param <R>
     * @return maped PairWorkloadOperator<K,R>
     */
    abstract public <R> PairWorkloadOperator<K, R> mapValue(MapFunction<V, R> fun, String componentId);

    abstract public <R> WorkloadOperator<R> map(MapFunction<Tuple2<K, V>, R> fun, String componentId);

    abstract public <R> WorkloadOperator<R> map(MapFunction<Tuple2<K, V>, R> fun, String componentId, Class<R> outputClass);

    abstract public <R> PairWorkloadOperator<K, R> flatMapValue(FlatMapFunction<V, R> fun, String componentId);

    abstract public PairWorkloadOperator<K, V> filter(FilterFunction<Tuple2<K, V>> fun, String componentId);

    abstract public PairWorkloadOperator<K, V> updateStateByKey(ReduceFunction<V> fun, String componentId);


    abstract public PairWorkloadOperator<K, V> reduceByKeyAndWindow(ReduceFunction<V> fun, String componentId, TimeDurations windowDuration);

    /**
     * Pre-aggregation -> key group -> reduce
     *
     * @param fun            reduce function
     * @param componentId
     * @param windowDuration
     * @param slideDuration
     * @return
     */
    abstract public PairWorkloadOperator<K, V> reduceByKeyAndWindow(ReduceFunction<V> fun, String componentId,
                                                                    TimeDurations windowDuration, TimeDurations slideDuration);

    abstract public WindowedPairWorkloadOperator<K, V> window(TimeDurations windowDuration);

    abstract public WindowedPairWorkloadOperator<K, V> window(TimeDurations windowDuration, TimeDurations slideDuration);

    /**
     * Join two pair streams which have the same type of key -- K
     *
     * @param joinStream         the other stream<K,R>
     * @param windowDuration     window length of this stream
     * @param joinWindowDuration window length of joinStream
     * @param <R>                Value type of joinStream
     * @return joined stream
     */
    abstract public <R> PairWorkloadOperator<K, Tuple2<V, R>> join(
            String componentId, PairWorkloadOperator<K, R> joinStream,
            TimeDurations windowDuration, TimeDurations joinWindowDuration) throws WorkloadException;

    /**
     * Join two pair streams which have the same type of key -- K base on event time
     *
     * @param joinStream         the other stream<K,R>
     * @param windowDuration     window length of this stream
     * @param windowDuration2    window length of joinStream
     * @param <R>                Value type of joinStream
     * @param eventTimeAssigner1 event time assignment for this stream
     * @param eventTimeAssigner2 event time assignment for joinStream
     * @return joined stream
     */
    abstract public <R> PairWorkloadOperator<K, Tuple2<V, R>> join(
            String componentId, PairWorkloadOperator<K, R> joinStream,
            TimeDurations windowDuration, TimeDurations windowDuration2,
            AssignTimeFunction<V> eventTimeAssigner1, AssignTimeFunction<R> eventTimeAssigner2) throws WorkloadException;

    abstract public void sink();
}

