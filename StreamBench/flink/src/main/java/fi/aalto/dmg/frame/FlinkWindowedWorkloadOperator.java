package fi.aalto.dmg.frame;

import fi.aalto.dmg.exceptions.UnsupportOperatorException;
import fi.aalto.dmg.frame.functions.*;
import fi.aalto.dmg.statistics.Throughput;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import scala.Tuple2;

/**
 * Created by yangjun.wang on 31/10/15.
 */
public class FlinkWindowedWorkloadOperator<T, W extends Window> extends WindowedWorkloadOperator<T> {

    private static final long serialVersionUID = -6316145102054759409L;
    private WindowedStream<T, T, W> windowStream;

    public FlinkWindowedWorkloadOperator(WindowedStream<T, T, W> stream, int parallelism) {
        super(parallelism);
        windowStream = stream;
    }

    @Override
    public <R> WorkloadOperator<R> mapPartition(final MapPartitionFunction<T, R> fun, final String componentId, final boolean logThroughput) {
        DataStream<R> newDataStream = this.windowStream.apply(new WindowFunction<T, R, T, W>() {
            Throughput throughput = new Throughput(componentId);

            @Override
            public void apply(T t, W window, Iterable<T> values, Collector<R> collector) throws Exception {
                if(logThroughput) {
                    throughput.execute();
                }
                Iterable<R> results = fun.mapPartition(values);
                for (R r : results) {
                    collector.collect(r);
                }
            }
        });
        return new FlinkWorkloadOperator<>(newDataStream, parallelism);
    }

    @Override
    public <R> WorkloadOperator<R> mapPartition(final MapPartitionFunction<T, R> fun, String componentId) {
        return mapPartition(fun, componentId, false);
    }

    @Override
    public <R> WorkloadOperator<R> map(final MapFunction<T, R> fun, final String componentId, final boolean logThroughput) {
        DataStream<R> newDataStream = this.windowStream.apply(new WindowFunction<T, R, T, W>() {
            Throughput throughput = new Throughput(componentId);

            @Override
            public void apply(T t, W window, Iterable<T> values, Collector<R> collector) throws Exception {
                if(logThroughput) {
                    throughput.execute();
                }
                for(T value : values){
                    R result = fun.map(value);
                    collector.collect(result);
                }
            }
        });
        return new FlinkWorkloadOperator<>(newDataStream, parallelism);
    }

    @Override
    public <R> WorkloadOperator<R> map(final MapFunction<T, R> fun, String componentId) {
        return map(fun, componentId, false);
    }

    @Override
    public WorkloadOperator<T> filter(final FilterFunction<T> fun, final String componentId, final boolean logThroughput) {
        DataStream<T> newDataStream = this.windowStream.apply(new WindowFunction<T, T, T, W>() {
            Throughput throughput = new Throughput(componentId);

            @Override
            public void apply(T t, W window, Iterable<T> values, Collector<T> collector) throws Exception {
                for(T value : values){
                    if(logThroughput) {
                        throughput.execute();
                    }
                    if(fun.filter(value))
                        collector.collect(value);
                }
            }
        });
        return new FlinkWorkloadOperator<>(newDataStream, parallelism);
    }

    @Override
    public WorkloadOperator<T> filter(final FilterFunction<T> fun, String componentId) {
        return filter(fun, componentId, false);
    }

    @Override
    public WorkloadOperator<T> reduce(final fi.aalto.dmg.frame.functions.ReduceFunction<T> fun, final String componentId, final boolean logThroughput) {
        DataStream<T> newDataStream = this.windowStream.reduce(new ReduceFunction<T>() {
            Throughput throughput = new Throughput(componentId);

            @Override
            public T reduce(T t, T t1) throws Exception {
                if(logThroughput) {
                    throughput.execute();
                }
                return fun.reduce(t, t1);
            }
        });
        return new FlinkWorkloadOperator<>(newDataStream, parallelism);
    }

    @Override
    public WorkloadOperator<T> reduce(final fi.aalto.dmg.frame.functions.ReduceFunction<T> fun, String componentId) {
        return reduce(fun, componentId, false);
    }

    @Override
    public <K, V> PairWorkloadOperator<K, V> mapToPair(final MapPairFunction<T, K, V> fun, final String componentId, final boolean logThroughput) {
        DataStream<Tuple2<K,V>> newDataStream = this.windowStream.apply(new WindowFunction<T, Tuple2<K, V>, T, W>() {
            Throughput throughput = new Throughput(componentId);

            @Override
            public void apply(T t, W window, Iterable<T> values, Collector<Tuple2<K, V>> collector) throws Exception {
                for(T value : values){
                    if(logThroughput){
                        throughput.execute();
                    }
                    Tuple2<K,V> result = fun.mapToPair(value);
                    collector.collect(result);
                }
            }
        });
        return new FlinkPairWorkloadOperator<K,V>(newDataStream, parallelism);
    }

    @Override
    public <K, V> PairWorkloadOperator<K, V> mapToPair(final MapPairFunction<T, K, V> fun, String componentId) {
        return mapToPair(fun, componentId, false);
    }

    @Override
    public void closeWith(OperatorBase stream, boolean broadcast) throws UnsupportOperatorException {
        throw new UnsupportOperatorException("not implemented yet");
    }

    @Override
    public void print() {

    }

}
