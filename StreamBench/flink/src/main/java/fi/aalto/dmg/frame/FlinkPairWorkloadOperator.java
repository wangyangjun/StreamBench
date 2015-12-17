package fi.aalto.dmg.frame;

import fi.aalto.dmg.exceptions.WorkloadException;
import fi.aalto.dmg.frame.functions.*;
import fi.aalto.dmg.frame.functions.FilterFunction;
import fi.aalto.dmg.frame.functions.FlatMapFunction;
import fi.aalto.dmg.frame.functions.MapFunction;
import fi.aalto.dmg.frame.functions.ReduceFunction;
import fi.aalto.dmg.statistics.Latency;
import fi.aalto.dmg.statistics.Throughput;
import fi.aalto.dmg.util.TimeDurations;
import fi.aalto.dmg.util.WithTime;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import scala.Tuple2;

import java.util.concurrent.TimeUnit;

/**
 * Created by yangjun.wang on 24/10/15.
 */
public class FlinkPairWorkloadOperator<K,V> implements PairWorkloadOperator<K,V> {

    private static final long serialVersionUID = 6796359288214662089L;
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

    @Override
    public PairWorkloadOperator<K, V> reduceByKey(final ReduceFunction<V> fun, final String componentId, final boolean logThroughput) {
        DataStream<Tuple2<K, V>> newDataStream = this.dataStream.keyBy(new KeySelector<Tuple2<K,V>, Object>() {
            @Override
            public Object getKey(Tuple2<K, V> value) throws Exception {
                return value._1();
            }
        }).reduce(new org.apache.flink.api.common.functions.ReduceFunction<Tuple2<K, V>>() {
            Throughput throughput = new Throughput(componentId);

            @Override
            public Tuple2<K, V> reduce(Tuple2<K, V> t1, Tuple2<K, V> t2) throws Exception {
                if(logThroughput) {
                    throughput.execute();
                }
                return new Tuple2<>(t1._1(), fun.reduce(t1._2(), t2._2()));
            }
        });
        return new FlinkPairWorkloadOperator<>(newDataStream);
    }

    // TODO: reduceByKey - reduce first then groupByKey, at last reduce again
    public PairWorkloadOperator<K, V> reduceByKey(final ReduceFunction<V> fun, String componentId) {
        return reduceByKey(fun, componentId, false);
    }

    @Override
    public <R> PairWorkloadOperator<K, R> mapValue(final MapFunction<V, R> fun, final String componentId, final boolean logThroughput) {
        DataStream<Tuple2<K,R>> newDataStream = dataStream.map(new org.apache.flink.api.common.functions.MapFunction<Tuple2<K, V>, Tuple2<K, R>>() {
            Throughput throughput = new Throughput(componentId);

            @Override
            public Tuple2<K, R> map(Tuple2<K, V> tuple2) throws Exception {
                if(logThroughput) {
                    throughput.execute();
                }
                return new Tuple2<>(tuple2._1(), fun.map(tuple2._2()));
            }
        });
        return new FlinkPairWorkloadOperator<>(newDataStream);
    }

    @Override
    public <R> PairWorkloadOperator<K, R> mapValue(final MapFunction<V, R> fun, String componentId) {
        return mapValue(fun, componentId, false);
    }

    @Override
    public <R> PairWorkloadOperator<K, R> flatMapValue(final FlatMapFunction<V, R> fun, final String componentId, final boolean logThroughput) {
        DataStream<Tuple2<K,R>> newDataStream = dataStream.flatMap(new org.apache.flink.api.common.functions.FlatMapFunction<Tuple2<K, V>, Tuple2<K, R>>() {
            Throughput throughput = new Throughput(componentId);

            @Override
            public void flatMap(Tuple2<K, V> tuple2, Collector<Tuple2<K, R>> collector) throws Exception {
                if(logThroughput) {
                    throughput.execute();
                }
                Iterable<R> rIterable = fun.flatMap(tuple2._2());
                for (R r : rIterable)
                    collector.collect(new Tuple2<>(tuple2._1(), r));
            }
        });
        return new FlinkPairWorkloadOperator<>(newDataStream);
    }

    @Override
    public <R> PairWorkloadOperator<K, R> flatMapValue(final FlatMapFunction<V, R> fun, String componentId) {
        return flatMapValue(fun, componentId, false);
    }

    @Override
    public PairWorkloadOperator<K, V> filter(final FilterFunction<Tuple2<K, V>> fun, final String componentId, final boolean logThroughput) {
        DataStream<Tuple2<K,V>> newDataStream = dataStream.filter(new org.apache.flink.api.common.functions.FilterFunction<Tuple2<K, V>>() {
            Throughput throughput = new Throughput(componentId);

            @Override
            public boolean filter(Tuple2<K, V> kvTuple2) throws Exception {
                if(logThroughput) {
                    throughput.execute();
                }
                return fun.filter(kvTuple2);
            }
        });
        return new FlinkPairWorkloadOperator<>(newDataStream);
    }

    @Override
    public PairWorkloadOperator<K, V> filter(final FilterFunction<scala.Tuple2<K, V>> fun, String componentId) {
        return filter(fun, componentId, false);
    }

    @Override
    public PairWorkloadOperator<K, V> iterative(MapFunction<V, V> mapFunction, final FilterFunction<Tuple2<K, V>> iterativeFunction, String componentId) {
        IterativeStream<Tuple2<K,V>> iteration = dataStream.iterate();

        DataStream<Tuple2<K,V>> mapedStream = iteration.map(new org.apache.flink.api.common.functions.MapFunction<Tuple2<K, V>, Tuple2<K, V>>() {
            @Override
            public Tuple2<K, V> map(Tuple2<K, V> value) throws Exception {
                return null;
            }
        });
        DataStream<Tuple2<K,V>> iterativeStream = mapedStream.filter(new org.apache.flink.api.common.functions.FilterFunction<Tuple2<K,V>>() {
            @Override
            public boolean filter(Tuple2<K,V> value) throws Exception {
                return iterativeFunction.filter(value);
            }
        });
        iteration.closeWith(iterativeStream);

        DataStream<Tuple2<K,V>> outputStream = mapedStream.filter(new org.apache.flink.api.common.functions.FilterFunction<Tuple2<K,V>>() {
            @Override
            public boolean filter(Tuple2<K,V> value) throws Exception {
                return !iterativeFunction.filter(value);
            }
        });
        return new FlinkPairWorkloadOperator<>(outputStream);
    }

    @Override
    public PairWorkloadOperator<K, V> updateStateByKey(ReduceFunction<V> fun, String componentId, boolean logThroughput) {
        return updateStateByKey(fun, componentId);
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
    public PairWorkloadOperator<K, V> reduceByKeyAndWindow(ReduceFunction<V> fun, String componentId, TimeDurations windowDuration, boolean logThroughput) {
        return reduceByKeyAndWindow(fun, componentId, windowDuration, windowDuration, logThroughput);
    }

    @Override
    public PairWorkloadOperator<K, V> reduceByKeyAndWindow(ReduceFunction<V> fun, String componentId, TimeDurations windowDuration) {
        return reduceByKeyAndWindow(fun, componentId, windowDuration, false);
    }

    @Override
    public PairWorkloadOperator<K, V> reduceByKeyAndWindow(final ReduceFunction<V> fun, final String componentId, TimeDurations windowDuration, TimeDurations slideDuration, final boolean logThroughput) {
        DataStream<Tuple2<K, V>> newDataStream = dataStream.keyBy(new KeySelector<Tuple2<K,V>, Object>() {
            @Override
            public K getKey(Tuple2<K, V> value) throws Exception {
                return value._1();
            }
        }).timeWindow(Time.of(windowDuration.getLength(), windowDuration.getUnit()), Time.of(slideDuration.getLength(), slideDuration.getUnit()))
                .reduce(new org.apache.flink.api.common.functions.ReduceFunction<Tuple2<K, V>>() {
                    Throughput throughput = new Throughput(componentId);

                    @Override
                    public Tuple2<K, V> reduce(Tuple2<K, V> t1, Tuple2<K, V> t2) throws Exception {
                        if(logThroughput) {
                            throughput.execute();
                        }
                        V result = fun.reduce(t1._2(), t2._2());
                        return new Tuple2<K, V>(t1._1(), result);
                    }
                });
        return new FlinkPairWorkloadOperator<>(newDataStream);
    }

    @Override
    public PairWorkloadOperator<K, V> reduceByKeyAndWindow(final ReduceFunction<V> fun, String componentId, TimeDurations windowDuration, TimeDurations slideDuration) {
        return reduceByKeyAndWindow(fun, componentId, windowDuration, slideDuration, false);
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
        return new FlinkWindowedPairWorkloadOperator<>(windowedDataStream);
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
    public <R> PairWorkloadOperator<K, scala.Tuple2<V, R>> join(String componentId, PairWorkloadOperator<K, R> joinStream, TimeDurations windowDuration, TimeDurations joinWindowDuration) throws WorkloadException {
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
    public <R> PairWorkloadOperator<K, Tuple2<V, R>> join(String componentId, PairWorkloadOperator<K, R> joinStream, TimeDurations windowDuration, TimeDurations joinWindowDuration, final AssignTimeFunction<V> eventTimeAssigner1, final AssignTimeFunction<R> eventTimeAssigner2) throws WorkloadException {
        StreamExecutionEnvironment env = this.dataStream.getExecutionEnvironment();
        if(null != eventTimeAssigner1 && null != eventTimeAssigner2)
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        else
            env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        if(windowDuration.equals(joinWindowDuration)){
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

                DataStream<Tuple2<K,V>> dataStream1 = this.dataStream
                    .keyBy(keySelector1);
                if(null != eventTimeAssigner1)
                    dataStream1 = dataStream1
                    .assignTimestamps(new AscendingTimestampExtractor<Tuple2<K, V>>() {
                        @Override
                        public long extractAscendingTimestamp(Tuple2<K, V> element, long currentTimestamp) {
                            return eventTimeAssigner1.assign(element._2());
                        }
                    });

                DataStream<Tuple2<K, R>> dataStream2 = joinFlinkStream.dataStream
                    .keyBy(keySelector2);
                if(null != eventTimeAssigner2)
                    dataStream2 = dataStream2
                    .assignTimestamps(new AscendingTimestampExtractor<Tuple2<K, R>>() {
                        @Override
                        public long extractAscendingTimestamp(Tuple2<K, R> element, long currentTimestamp) {
                            return eventTimeAssigner2.assign(element._2());
                        }
                    });

                DataStream<Tuple2<K, Tuple2<V, R>>> joineStream =
                        new NoWindowJoinedStreams<>(dataStream1, dataStream2)
                        .where(keySelector1)
                        .buffer(Time.of(30, TimeUnit.SECONDS))
                        .equalTo(keySelector2)
                        .buffer(Time.of(30, TimeUnit.SECONDS))
                        .apply(new JoinFunction<Tuple2<K,V>, Tuple2<K,R>, Tuple2<K, Tuple2<V,R>>>() {
                            @Override
                            public Tuple2<K, Tuple2<V, R>> join(Tuple2<K, V> first, Tuple2<K, R> second) throws Exception {
                                return new Tuple2<>(first._1(), new Tuple2<V, R>(first._2(), second._2()));
                            }
                        });

                return new FlinkPairWorkloadOperator<>(joineStream);
            } else {
                throw new WorkloadException("Cast joinStrem to FlinkPairWorkloadOperator failed");
            }
        } else {
            throw new WorkloadException("Flink only supports join operation on the same window1");
        }
    }

    @Override
    public void print() {
        this.dataStream.print();
    }

    @Override
    public void sink() {
        this.dataStream.addSink(new SinkFunction<Tuple2<K, V>>() {
            Latency latency = new Latency("sink");

            @Override
            public void invoke(Tuple2<K, V> value) throws Exception {
                latency.execute((WithTime<? extends Object>) value._2());
            }
        });
    }
}

