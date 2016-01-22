package fi.aalto.dmg.frame;

import fi.aalto.dmg.frame.functions.*;
import fi.aalto.dmg.frame.functions.FilterFunction;
import fi.aalto.dmg.frame.functions.FlatMapFunction;
import fi.aalto.dmg.frame.functions.MapFunction;
import fi.aalto.dmg.frame.functions.ReduceFunction;
import fi.aalto.dmg.statistics.Latency;
import fi.aalto.dmg.statistics.Throughput;
import fi.aalto.dmg.util.TimeDurations;
import fi.aalto.dmg.util.WithTime;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import scala.Tuple2;

/**
 * Created by yangjun.wang on 24/10/15.
 */
public class FlinkWorkloadOperator<T> extends OperatorBase implements WorkloadOperator<T> {
    private static final long serialVersionUID = -8568404720313488006L;
    protected DataStream<T> dataStream;


    public FlinkWorkloadOperator(DataStream<T> dataSet1){
        dataStream = dataSet1;
    }

    @Override
    public <R> WorkloadOperator<R> map(final MapFunction<T, R> fun, final String componentId, int parallelism, final boolean logThroughput) {
        DataStream<R> newDataStream = dataStream.map(new org.apache.flink.api.common.functions.MapFunction<T, R>() {
            Throughput throughput = new Throughput(componentId);
            public R map(T t) throws Exception {
                if(logThroughput){
                    throughput.execute();
                }
                return fun.map(t);
            }
        });
        return new FlinkWorkloadOperator<>(newDataStream);
    }

    @Override
    public <R> WorkloadOperator<R> map(final MapFunction<T, R> fun, String componentId, int parallelism) {
        return map(fun, componentId, parallelism, false);
    }

    @Override
    public <K, V> PairWorkloadOperator<K, V> mapToPair(final MapPairFunction<T, K, V> fun, final String componentId, int parallelism, final boolean logThroughput) {
        DataStream<Tuple2<K,V>> newDataStream = dataStream.map(new org.apache.flink.api.common.functions.MapFunction<T, Tuple2<K, V>>() {
            Throughput throughput = new Throughput(componentId);

            public Tuple2<K, V> map(T t) throws Exception {
                if(logThroughput) {
                    throughput.execute();
                }
                scala.Tuple2<K,V> tuple2 = fun.mapToPair(t);
                return new Tuple2<>(tuple2._1(), tuple2._2());
            }
        });
        return new FlinkPairWorkloadOperator<K,V>(newDataStream);
    }

    @Override
    public <K, V> PairWorkloadOperator<K, V> mapToPair(final MapPairFunction<T, K, V> fun, String componentId, int parallelism) {
        return mapToPair(fun, componentId, parallelism, false);
    }

    @Override
    public WorkloadOperator<T> reduce(final ReduceFunction<T> fun, final String componentId, int parallelism, final boolean logThroughput) {
        DataStream<T> newDataStream = dataStream.keyBy(0).reduce(new org.apache.flink.api.common.functions.ReduceFunction<T>() {
            Throughput throughput = new Throughput(componentId);
            public T reduce(T t, T t1) throws Exception {
                if(logThroughput){
                    throughput.execute();
                }
                return fun.reduce(t, t1);
            }
        });
        return new FlinkWorkloadOperator<>(newDataStream);
    }

    @Override
    public WorkloadOperator<T> reduce(final ReduceFunction<T> fun, String componentId, int parallelism) {
        return  reduce(fun, componentId, parallelism, false);
    }

    @Override
    public WorkloadOperator<T> filter(final FilterFunction<T> fun, final String componentId, int parallelism, final boolean logThroughput) {
        DataStream<T> newDataStream = dataStream.filter(new org.apache.flink.api.common.functions.FilterFunction<T>() {
            Throughput throughput = new Throughput(componentId);
            public boolean filter(T t) throws Exception {
                if(logThroughput){
                    throughput.execute();
                }
                return fun.filter(t);
            }
        });
        return new FlinkWorkloadOperator<>(newDataStream);
    }

    @Override
    public WorkloadOperator<T> filter(final FilterFunction<T> fun, String componentId, int parallelism) {
        return filter(fun, componentId, parallelism, false);
    }

    @Override
    public WorkloadOperator<T> iterative(final MapFunction<T, T> mapFunction, final FilterFunction<T> iterativeFunction, String componentId, int parallelism) {
        IterativeStream<T> iteration = dataStream.iterate();

        DataStream<T> mapedStream = iteration.map(new org.apache.flink.api.common.functions.MapFunction<T, T>() {
            @Override
            public T map(T value) throws Exception {
                return mapFunction.map(value);
            }
        });
        DataStream<T> iterativeStream = mapedStream.filter(new org.apache.flink.api.common.functions.FilterFunction<T>() {
            @Override
            public boolean filter(T value) throws Exception {
                return iterativeFunction.filter(value);
            }
        });
        iteration.closeWith(iterativeStream);

        DataStream<T> outputStream = mapedStream.filter(new org.apache.flink.api.common.functions.FilterFunction<T>() {
            @Override
            public boolean filter(T value) throws Exception {
                return !iterativeFunction.filter(value);
            }
        });
        return new FlinkWorkloadOperator<>(outputStream);
    }

    @Override
    public <R> WorkloadOperator<R> flatMap(final FlatMapFunction<T, R> fun, final String componentId, int parallelism, final boolean logThroughput) {
        TypeInformation<R> returnType = TypeExtractor.createTypeInfo(FlatMapFunction.class, fun.getClass(), 1, null, null);
        final Throughput throughput = new Throughput(componentId);

        DataStream<R> newDataStream = dataStream.flatMap(new org.apache.flink.api.common.functions.FlatMapFunction<T, R>() {
            public void flatMap(T t, Collector<R> collector) throws Exception {
                if(logThroughput) {
                    throughput.execute();
                }
                java.lang.Iterable<R> flatResults = fun.flatMap(t);
                for(R r : flatResults){
                    collector.collect(r);
                }
            }
        }).returns(returnType);
        return new FlinkWorkloadOperator<>(newDataStream);
    }

    @Override
    public <R> WorkloadOperator<R> flatMap(final FlatMapFunction<T, R> fun, String componentId, int parallelism) {
        return flatMap(fun, componentId, parallelism, false);
    }

    @Override
    public <K, V> PairWorkloadOperator<K, V> flatMapToPair(final FlatMapPairFunction<T, K, V> fun,
                                                           String componentId,
                                                           int parallelism,
                                                           final boolean logThroughput) {

//        TypeInformation returnType = TypeExtractor.createTypeInfo(FlatMapFunction.class, fun.getClass(), 1, null, null);
        final Throughput throughput = new Throughput(componentId);

        DataStream<Tuple2<K,V>> newDataStream = dataStream.flatMap(new org.apache.flink.api.common.functions.FlatMapFunction<T, Tuple2<K, V>>() {
            public void flatMap(T t, Collector<Tuple2<K, V>> collector) throws Exception {
                if(logThroughput) {
                    throughput.execute();
                }
                java.lang.Iterable<Tuple2<K,V>> flatResults = fun.flatMapToPair(t);
                for(Tuple2<K,V> tuple2 : flatResults){
                    collector.collect(tuple2);
                }
            }
        });
        return new FlinkPairWorkloadOperator<>(newDataStream);
    }

    @Override
    public <K, V> PairWorkloadOperator<K, V> flatMapToPair(FlatMapPairFunction<T, K, V> fun,
                                                           String componentId,
                                                           int parallelism) {
        return flatMapToPair(fun, componentId, parallelism, false);
    }

    @Override
    public WindowedWorkloadOperator<T> window(TimeDurations windowDuration) {
        return window(windowDuration, windowDuration);
    }

    @Override
    public WindowedWorkloadOperator<T> window(TimeDurations windowDuration, TimeDurations slideDuration) {
        WindowedStream<T, T, TimeWindow> windowedStream = dataStream.keyBy(new KeySelector<T, T>() {
            @Override
            public T getKey(T value) throws Exception {
                return value;
            }
        }).timeWindow(Time.of(windowDuration.getLength(), windowDuration.getUnit()),
                Time.of(slideDuration.getLength(), slideDuration.getUnit()));
        return new FlinkWindowedWorkloadOperator<>(windowedStream);
    }

    public void print() {
        this.dataStream.print();
    }

    @Override
    public void sink(int parallelism) {
        this.dataStream.addSink(new SinkFunction<T>() {
            Latency latency = new Latency("sink");
            Throughput throughput = new Throughput("sink");

            @Override
            public void invoke(T value) throws Exception {
//                latency.execute((WithTime<? extends Object>) value);
                throughput.execute();
            }
        });
    }
}
