package fi.aalto.dmg.frame;

import api.datastream.NoWindowJoinedStreams;
import fi.aalto.dmg.exceptions.UnsupportOperatorException;
import fi.aalto.dmg.exceptions.WorkloadException;
import fi.aalto.dmg.frame.functions.*;
import fi.aalto.dmg.frame.functions.FilterFunction;
import fi.aalto.dmg.frame.functions.FlatMapFunction;
import fi.aalto.dmg.frame.functions.MapFunction;
import fi.aalto.dmg.frame.functions.ReduceFunction;
import fi.aalto.dmg.statistics.LatencyLog;
import fi.aalto.dmg.util.TimeDurations;
import fi.aalto.dmg.util.WithTime;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.TimestampExtractor;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.Tuple2;

import java.util.concurrent.TimeUnit;

/**
 * Created by yangjun.wang on 24/10/15.
 */
public class FlinkPairWorkloadOperator<K,V> extends PairWorkloadOperator<K,V> {

    private static final long serialVersionUID = 6796359288214662089L;
    private DataStream<Tuple2<K, V>> dataStream;

    public FlinkPairWorkloadOperator(DataStream<Tuple2<K, V>> dataStream1, int parallelism) {
        super(parallelism);
        this.dataStream = dataStream1;
    }


    public FlinkGroupedWorkloadOperator<K, V> groupByKey() {
        KeyedStream<Tuple2<K, V>, Object> keyedStream = this.dataStream.keyBy(new KeySelector<Tuple2<K, V>, Object>() {
            @Override
            public K getKey(Tuple2<K, V> value) throws Exception {
                return value._1();
            }
        });
        return new FlinkGroupedWorkloadOperator<>(keyedStream, parallelism);
    }

    // TODO: reduceByKey - reduce first then groupByKey, at last reduce again
    @Override
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
        return new FlinkPairWorkloadOperator<>(newDataStream, parallelism);
    }

    @Override
    public <R> PairWorkloadOperator<K, R> mapValue(final MapFunction<V, R> fun, String componentId) {
        DataStream<Tuple2<K,R>> newDataStream = dataStream.map(new org.apache.flink.api.common.functions.MapFunction<Tuple2<K, V>, Tuple2<K, R>>() {
            @Override
            public Tuple2<K, R> map(Tuple2<K, V> tuple2) throws Exception {
                return new Tuple2<>(tuple2._1(), fun.map(tuple2._2()));
            }
        });
        return new FlinkPairWorkloadOperator<>(newDataStream, parallelism);
    }

    @Override
    public <R> WorkloadOperator<R> map(final MapFunction<Tuple2<K, V>, R> fun, final String componentId) {
        DataStream<R> newDataStream = dataStream.map(new org.apache.flink.api.common.functions.MapFunction<Tuple2<K, V>, R>() {
            @Override
            public R map(Tuple2<K, V> value) throws Exception {
                return fun.map(value);
            }
        });
        return new FlinkWorkloadOperator<>(newDataStream, parallelism);
    }

    @Override
    public <R> WorkloadOperator<R> map(final MapFunction<Tuple2<K, V>, R> fun, final String componentId, Class<R> outputClass) {
        org.apache.flink.api.common.functions.MapFunction<Tuple2<K, V>, R> mapper
                = new org.apache.flink.api.common.functions.MapFunction<Tuple2<K, V>, R>() {
            @Override
            public R map(Tuple2<K, V> value) throws Exception {
                return fun.map(value);
            }
        };
        TypeInformation<R> outType = TypeExtractor.getForClass(outputClass);
        DataStream<R> newDataStream = dataStream.transform("Map", outType,
                new StreamMap<>(dataStream.getExecutionEnvironment().clean(mapper)));
        return new FlinkWorkloadOperator<>(newDataStream, parallelism);
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
        return new FlinkPairWorkloadOperator<>(newDataStream, parallelism);
    }

    @Override
    public PairWorkloadOperator<K, V> filter(final FilterFunction<scala.Tuple2<K, V>> fun, String componentId) {
        DataStream<Tuple2<K,V>> newDataStream = dataStream.filter(new org.apache.flink.api.common.functions.FilterFunction<Tuple2<K, V>>() {
            @Override
            public boolean filter(Tuple2<K, V> kvTuple2) throws Exception {
                return fun.filter(kvTuple2);
            }
        });
        return new FlinkPairWorkloadOperator<>(newDataStream, parallelism);
    }

    /**
     * Whether is windowed stream?
     * @param fun reduce function
     * @param componentId current component id
     * @return PairWorkloadOperator
     */
    public PairWorkloadOperator<K, V> updateStateByKey(ReduceFunction<V> fun, String componentId) {
        return this;
    }


    @Override
    public PairWorkloadOperator<K, V> reduceByKeyAndWindow(ReduceFunction<V> fun, String componentId, TimeDurations windowDuration) {
        return reduceByKeyAndWindow(fun, componentId, windowDuration);
    }

    // TODO: implement pre-aggregation
    /**
     * Pre-aggregation -> key group -> reduce
     * @param fun
     *      reduce function
     * @param componentId
     * @param windowDuration
     * @param slideDuration
     * @return
     */
    @Override
    public PairWorkloadOperator<K, V> reduceByKeyAndWindow(final ReduceFunction<V> fun, String componentId, TimeDurations windowDuration, TimeDurations slideDuration) {
        org.apache.flink.api.common.functions.ReduceFunction<Tuple2<K, V>> reduceFunction =
                new org.apache.flink.api.common.functions.ReduceFunction<Tuple2<K, V>>() {
                    @Override
                    public Tuple2<K, V> reduce(Tuple2<K, V> t1, Tuple2<K, V> t2) throws Exception {
                        V result = fun.reduce(t1._2(), t2._2());
                        return new Tuple2<K, V>(t1._1(), result);
                    }
                };
        DataStream<Tuple2<K, V>> newDataStream = dataStream
                .keyBy(new KeySelector<Tuple2<K,V>, Object>() {
                    @Override
                    public K getKey(Tuple2<K, V> value) throws Exception {
                        return value._1();
                    }
                })
                .timeWindow(Time.of(windowDuration.getLength(),
                        windowDuration.getUnit()),
                        Time.of(slideDuration.getLength(),
                                slideDuration.getUnit()))
                .reduce(reduceFunction);
        return new FlinkPairWorkloadOperator<>(newDataStream, parallelism);
    }

    @Override
    public WindowedPairWorkloadOperator<K,V> window(TimeDurations windowDuration) {
        return window(windowDuration, windowDuration);
    }

    /**
     * Keyed stream window1
     * @param windowDuration window1 size
     * @param slideDuration slide size
     * @return WindowedPairWorkloadOperator
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
        return new FlinkWindowedPairWorkloadOperator<>(windowedDataStream, parallelism);
    }

    /**
     *
     * @param joinStream
     *          the other stream<K,R>
     * @param windowDuration
     *          window1 length of this stream
     * @param joinWindowDuration
     *          window1 length of joinStream
     * @param <R> Value type of the other stream
     * @return PairWorkloadOperator after join
     */
    @Override
    public <R> PairWorkloadOperator<K, scala.Tuple2<V, R>> join(String componentId,
                                                                PairWorkloadOperator<K, R> joinStream,
                                                                TimeDurations windowDuration,
                                                                TimeDurations joinWindowDuration) throws WorkloadException {
        return join(componentId, joinStream, windowDuration, joinWindowDuration, null, null);
    }

    /**
     * Event time join
     * @param componentId current compute component id
     * @param joinStream
     *          the other stream<K,R>
     * @param windowDuration
     *          window1 length of this stream
     * @param joinWindowDuration
     *          window1 length of joinStream
     * @param eventTimeAssigner1
     *          event time assignment for this stream
     * @param eventTimeAssigner2
     *          event time assignment for joinStream
     * @param <R> Value type of the other stream
     * @return PairWorkloadOperator
     * @throws WorkloadException
     */
    @Override
    public <R> PairWorkloadOperator<K, Tuple2<V, R>> join(String componentId,
                                                          PairWorkloadOperator<K, R> joinStream,
                                                          TimeDurations windowDuration,
                                                          TimeDurations joinWindowDuration,
                                                          final AssignTimeFunction<V> eventTimeAssigner1,
                                                          final AssignTimeFunction<R> eventTimeAssigner2) throws WorkloadException {
        StreamExecutionEnvironment env = this.dataStream.getExecutionEnvironment();
        if(null != eventTimeAssigner1 && null != eventTimeAssigner2)
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        else
            env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        if(joinStream instanceof FlinkPairWorkloadOperator){
            FlinkPairWorkloadOperator<K,R> joinFlinkStream = ((FlinkPairWorkloadOperator<K,R>) joinStream);

            final KeySelector<Tuple2<K,V>, K> keySelector1 = new KeySelector<Tuple2<K,V>, K>() {
                @Override
                public K getKey(Tuple2<K, V> kvTuple2) throws Exception {
                    return kvTuple2._1();
                }
            };
            final KeySelector<Tuple2<K,R>, K> keySelector2 = new KeySelector<Tuple2<K,R>, K>() {
                @Override
                public K getKey(Tuple2<K, R> krTuple2) throws Exception {
                    return krTuple2._1();
                }
            };

            // tmpKeySelector for get keyTypeInfo InvalidTypesException
            // TODO: find a better solution to solve
            KeySelector<K,K> tmpKeySelector = new KeySelector<K, K>() {
                @Override
                public K getKey(K value) throws Exception {
                    return value;
                }
            };
            TypeInformation<K> keyTypeInfo = TypeExtractor.getUnaryOperatorReturnType(tmpKeySelector, KeySelector.class, false, false, dataStream.getType(), null ,false);

            DataStream<Tuple2<K,V>> dataStream1 = new KeyedStream<>(dataStream, keySelector1, keyTypeInfo);
            if(null != eventTimeAssigner1) {
//                final AscendingTimestampExtractor<Tuple2<K, V>> timestampExtractor1 = new AscendingTimestampExtractor<Tuple2<K, V>>() {
//                    @Override
//                    public long extractAscendingTimestamp(Tuple2<K, V> element, long currentTimestamp) {
//                        return eventTimeAssigner1.assign(element._2());
//                    }
//                };
                TimestampExtractor<Tuple2<K,V>> timestampExtractor1 = new TimestampExtractor<Tuple2<K, V>>() {
                    long currentTimestamp = 0L;
                    @Override
                    public long extractTimestamp(Tuple2<K, V> kvTuple2, long l) {
                        long timestamp = eventTimeAssigner1.assign(kvTuple2._2());
                        if( timestamp > this.currentTimestamp) {
                            this.currentTimestamp = timestamp;
                        }
                        return timestamp;
                    }

                    @Override
                    public long extractWatermark(Tuple2<K, V> kvTuple2, long l) {
                        return -9223372036854775808L;
                    }

                    @Override
                    public long getCurrentWatermark() {
                        return currentTimestamp - 500;
                    }
                };
                dataStream1 = dataStream1.assignTimestamps(timestampExtractor1);
            }

            DataStream<Tuple2<K, R>> dataStream2 = new KeyedStream<>(joinFlinkStream.dataStream, keySelector2, keyTypeInfo);
            if(null != eventTimeAssigner2) {
//                final AscendingTimestampExtractor<Tuple2<K, R>> timestampExtractor2 = new AscendingTimestampExtractor<Tuple2<K, R>>() {
//                    @Override
//                    public long extractAscendingTimestamp(Tuple2<K, R> element, long currentTimestamp) {
//                        return eventTimeAssigner2.assign(element._2());
//                    }
//                };
                TimestampExtractor<Tuple2<K,R>> timestampExtractor2 = new TimestampExtractor<Tuple2<K, R>>() {
                    long currentTimestamp = 0L;
                    @Override
                    public long extractTimestamp(Tuple2<K, R> kvTuple2, long l) {
                        long timestamp = eventTimeAssigner2.assign(kvTuple2._2());
                        if( timestamp > this.currentTimestamp) {
                            this.currentTimestamp = timestamp;
                        }
                        return timestamp;
                    }

                    @Override
                    public long extractWatermark(Tuple2<K, R> kvTuple2, long l) {
                        return -9223372036854775808L;
                    }

                    @Override
                    public long getCurrentWatermark() {
                        return currentTimestamp - 500;
                    }
                };
                dataStream2 = dataStream2.assignTimestamps(timestampExtractor2);
            }

            DataStream<Tuple2<K, Tuple2<V, R>>> joineStream =
                    new NoWindowJoinedStreams<>(dataStream1, dataStream2)
                    .where(keySelector1, keyTypeInfo)
                    .buffer(Time.of(windowDuration.toMilliSeconds(), TimeUnit.MILLISECONDS))
                    .equalTo(keySelector2, keyTypeInfo)
                    .buffer(Time.of(joinWindowDuration.toMilliSeconds(), TimeUnit.MILLISECONDS))
                    .apply(new JoinFunction<Tuple2<K,V>, Tuple2<K,R>, Tuple2<K, Tuple2<V,R>>>() {
                        @Override
                        public Tuple2<K, Tuple2<V, R>> join(Tuple2<K, V> first, Tuple2<K, R> second) throws Exception {
                            return new Tuple2<>(first._1(), new Tuple2<>(first._2(), second._2()));
                        }
                    });

            return new FlinkPairWorkloadOperator<>(joineStream, parallelism);
        } else {
            throw new WorkloadException("Cast joinStrem to FlinkPairWorkloadOperator failed");
        }
    }

    @Override
    public void closeWith(OperatorBase stream, boolean broadcast) throws UnsupportOperatorException {

    }

    @Override
    public void print() {
        this.dataStream.print();
    }

    @Override
    public void sink() {
//        this.dataStream.print();
        this.dataStream.addSink(new SinkFunction<Tuple2<K, V>>() {
            LatencyLog latency = new LatencyLog("sink");

            @Override
            public void invoke(Tuple2<K, V> value) throws Exception {
                latency.execute((WithTime<? extends Object>) value._2());
            }
        });
    }
}

