package fi.aalto.dmg.frame;

import fi.aalto.dmg.exceptions.WorkloadException;
import fi.aalto.dmg.frame.functions.FilterFunction;
import fi.aalto.dmg.frame.functions.FlatMapFunction;
import fi.aalto.dmg.frame.functions.MapFunction;
import fi.aalto.dmg.frame.functions.ReduceFunction;
import fi.aalto.dmg.util.TimeDurations;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.windowing.assigners.TumblingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.Tuple2;

import java.lang.reflect.TypeVariable;

/**
 * Created by yangjun.wang on 24/10/15.
 */
public class FlinkPairWorkloadOperator<K,V> implements PairWorkloadOperator<K,V> {

    private DataStream<Tuple2<K, V>> dataStream;

    public FlinkPairWorkloadOperator(DataStream<Tuple2<K, V>> dataStream1) {
        this.dataStream = dataStream1;
    }



    public FlinkGroupedWorkloadOperator<K, V> groupByKey() {
        KeyedStream<Tuple2<K, V>, Object> keyedStream = this.dataStream.keyBy(new KeySelector<Tuple2<K, V>, Object>() {
            @Override
            public K getKey(Tuple2<K, V> value) throws Exception {
                return value._1();
            }
        });
        return new FlinkGroupedWorkloadOperator<>(keyedStream);
    }

    // TODO: reduceByKey - reduce first then groupByKey, at last reduce again
    // ReduceFunction is always combinable
    public PairWorkloadOperator<K, V> reduceByKey(final ReduceFunction<V> fun, String componentId) {
        DataStream<Tuple2<K, V>> newDataStream = this.dataStream.keyBy(new KeySelector<Tuple2<K,V>, Object>() {
            @Override
            public Object getKey(Tuple2<K, V> value) throws Exception {
                return value._1();
            }
        }).reduce(new org.apache.flink.api.common.functions.ReduceFunction<Tuple2<K, V>>() {
            @Override
            public Tuple2<K, V> reduce(Tuple2<K, V> t1, Tuple2<K, V> t2) throws Exception {
                return new Tuple2<>(t1._1(), fun.reduce(t1._2(), t2._2()));
            }
        });
        return new FlinkPairWorkloadOperator<>(newDataStream);
    }

    @Override
    public <R> PairWorkloadOperator<K, R> mapValue(final MapFunction<V, R> fun, String componentId) {
        DataStream<Tuple2<K,R>> newDataStream = dataStream.map(new org.apache.flink.api.common.functions.MapFunction<Tuple2<K, V>, Tuple2<K, R>>() {
            @Override
            public Tuple2<K, R> map(Tuple2<K, V> tuple2) throws Exception {
                return new Tuple2<>(tuple2._1(), fun.map(tuple2._2()));
            }
        });
        return new FlinkPairWorkloadOperator<>(newDataStream);
    }

    @Override
    public <R> PairWorkloadOperator<K, R> flatMapValue(final FlatMapFunction<V, R> fun, String componentId) {
        DataStream<Tuple2<K,R>> newDataStream = dataStream.flatMap(new org.apache.flink.api.common.functions.FlatMapFunction<Tuple2<K, V>, Tuple2<K, R>>() {
            @Override
            public void flatMap(Tuple2<K, V> tuple2, Collector<Tuple2<K, R>> collector) throws Exception {
                Iterable<R> rIterable = fun.flatMap(tuple2._2());
                for (R r : rIterable)
                    collector.collect(new Tuple2<>(tuple2._1(), r));
            }
        });
        return new FlinkPairWorkloadOperator<>(newDataStream);
    }

    @Override
    public PairWorkloadOperator<K, V> filter(final FilterFunction<scala.Tuple2<K, V>> fun, String componentId) {
        DataStream<Tuple2<K,V>> newDataStream = dataStream.filter(new org.apache.flink.api.common.functions.FilterFunction<Tuple2<K, V>>() {
            @Override
            public boolean filter(Tuple2<K, V> kvTuple2) throws Exception {
                return fun.filter(kvTuple2);
            }
        });
        return new FlinkPairWorkloadOperator<>(newDataStream);
    }

    /**
     * Whether is windowed stream?
     * @param fun
     * @param componentId
     * @return
     */
    public PairWorkloadOperator<K, V> updateStateByKey(ReduceFunction<V> fun, String componentId) {
        return this;
    }

    @Override
    public PairWorkloadOperator<K, V> reduceByKeyAndWindow(ReduceFunction<V> fun, String componentId, TimeDurations windowDuration) {
        return reduceByKeyAndWindow(fun, componentId, windowDuration, windowDuration);
    }

    @Override
    public PairWorkloadOperator<K, V> reduceByKeyAndWindow(final ReduceFunction<V> fun, String componentId, TimeDurations windowDuration, TimeDurations slideDuration) {
        DataStream<Tuple2<K, V>> newDataStream = dataStream.keyBy(new KeySelector<Tuple2<K,V>, Object>() {
            @Override
            public K getKey(Tuple2<K, V> value) throws Exception {
                return value._1();
            }
        }).timeWindow(Time.of(windowDuration.getLength(), windowDuration.getUnit()), Time.of(slideDuration.getLength(), slideDuration.getUnit()))
                .reduce(new org.apache.flink.api.common.functions.ReduceFunction<Tuple2<K, V>>() {
                    @Override
                    public Tuple2<K, V> reduce(Tuple2<K, V> t1, Tuple2<K, V> t2) throws Exception {
                        V result = fun.reduce(t1._2(), t2._2());
                        return new Tuple2<K, V>(t1._1(), result);
                    }
                });
        return new FlinkPairWorkloadOperator<>(newDataStream);
    }

    @Override
    public WindowedPairWorkloadOperator<K,V> window(TimeDurations windowDuration) {
        return window(windowDuration, windowDuration);
    }

    /**
     * Keyed stream window
     * @param windowDuration window size
     * @param slideDuration slide size
     * @return
     */
    @Override
    public WindowedPairWorkloadOperator<K, V> window(TimeDurations windowDuration, TimeDurations slideDuration) {
        WindowedStream<Tuple2<K, V>, K, TimeWindow> windowedDataStream = dataStream.keyBy(new KeySelector<Tuple2<K,V>, K>() {
            @Override
            public K getKey(Tuple2<K, V> tuple2) throws Exception {
                return tuple2._1();
            }
        }).timeWindow(Time.of(windowDuration.getLength(), windowDuration.getUnit()),
                Time.of(slideDuration.getLength(), slideDuration.getUnit()));
        return new FlinkWindowedPairWorkloadOperator<>(windowedDataStream);
    }

    /**
     *
     * @param joinStream
     *          the other stream<K,R>
     * @param windowDuration
     *          window length of this stream
     * @param joinWindowDuration
     *          window length of joinStream
     * @param <R>
     * @return
     */
    @Override
    public <R> PairWorkloadOperator<K, scala.Tuple2<V, R>> join(String componentId, PairWorkloadOperator<K, R> joinStream, TimeDurations windowDuration, TimeDurations joinWindowDuration) throws WorkloadException {
        if(windowDuration.equals(joinWindowDuration)){
            if(joinStream instanceof FlinkPairWorkloadOperator){
                FlinkPairWorkloadOperator<K,R> joinFlinkStream = ((FlinkPairWorkloadOperator<K,R>) joinStream);
                DataStream<Tuple2<K, Tuple2<V, R>>> joinedStream = this.dataStream
                        .join((joinFlinkStream.dataStream))
                        .where(new KeySelector<Tuple2<K,V>, K>() {
                            @Override
                            public K getKey(Tuple2<K, V> kvTuple2) throws Exception {
                                return kvTuple2._1();
                            }
                        })
                        .equalTo(new KeySelector<Tuple2<K,R>, K>() {
                            @Override
                            public K getKey(Tuple2<K, R> krTuple2) throws Exception {
                                return krTuple2._1();
                            }
                        })
                        .window(TumblingTimeWindows.of(Time.of(windowDuration.getLength(), windowDuration.getUnit())))
                        .apply(new JoinFunction<Tuple2<K, V>, Tuple2<K, R>, Tuple2<K, Tuple2<V, R>>>() {
                            @Override
                            public Tuple2<K, Tuple2<V, R>> join(Tuple2<K, V> kvTuple2, Tuple2<K, R> krTuple2) throws Exception {
                                return new Tuple2<K, Tuple2<V, R>>(kvTuple2._1(), new Tuple2<V,R>(kvTuple2._2(), krTuple2._2()));
                            }
                        });
                return new FlinkPairWorkloadOperator<>(joinedStream);
            } else {
                throw new WorkloadException("Cast joinStrem to FlinkPairWorkloadOperator failed");
            }
        } else {
            throw new WorkloadException("Flink only supports join operation on the same window");
        }
    }


    @Override
    public void print() {
        this.dataStream.print();
    }
}

