package fi.aalto.dmg.frame;

import fi.aalto.dmg.frame.functions.*;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedDataStream;
import org.apache.flink.streaming.api.functions.WindowMapFunction;
import org.apache.flink.util.Collector;
import scala.Tuple2;

/**
 * Created by yangjun.wang on 31/10/15.
 */
public class FlinkWindowedWorkloadOperator<T> implements WindowedWorkloadOperator<T> {

    protected WindowedDataStream<T> dataStream;

    public FlinkWindowedWorkloadOperator(WindowedDataStream<T> dataStream1){
        dataStream = dataStream1;
    }

    public <R> WindowedWorkloadOperator<R> mapPartition(final MapPartitionFunction<T, R> fun, String componentId) {
        WindowedDataStream<R> newDataStream = dataStream.mapWindow(new WindowMapFunction<T, R>() {
            @Override
            public void mapWindow(Iterable<T> values, Collector<R> collector) throws Exception {
                Iterable<R> results = fun.mapPartition(values);
                for (R r : results) {
                    collector.collect(r);
                }
            }
        });
        return new FlinkWindowedWorkloadOperator<>(newDataStream);
    }

    @Override
    public <R> WindowedWorkloadOperator<R> map(final MapFunction<T, R> fun, String componentId) {
        WindowedDataStream<R> newDataStream = dataStream.mapWindow(new WindowMapFunction<T, R>() {
            @Override
            public void mapWindow(Iterable<T> values, Collector<R> collector) throws Exception {
                for(T value : values){
                    R result = fun.map(value);
                    collector.collect(result);
                }
            }
        });
        return new FlinkWindowedWorkloadOperator<>(newDataStream);
    }

    @Override
    public WindowedWorkloadOperator<T> filter(final FilterFunction<T> fun, String componentId) {
        WindowedDataStream<T> newDataStream = dataStream.mapWindow(new WindowMapFunction<T, T>() {
            @Override
            public void mapWindow(Iterable<T> values, Collector<T> collector) throws Exception {
                for(T value : values){
                    if(fun.filter(value))
                        collector.collect(value);
                }
            }
        });
        return new FlinkWindowedWorkloadOperator<>(newDataStream);
    }

    @Override
    public WindowedWorkloadOperator<T> reduce(final fi.aalto.dmg.frame.functions.ReduceFunction<T> fun, String componentId) {
        WindowedDataStream<T> newDataStream = dataStream.reduceWindow(new ReduceFunction<T>() {
            @Override
            public T reduce(T t, T t1) throws Exception {
                return fun.reduce(t, t1);
            }
        });
        return new FlinkWindowedWorkloadOperator<>(newDataStream);
    }

    @Override
    public <K, V> WindowedPairWorkloadOperator<K, V> mapToPair(final MapPairFunction<T, K, V> fun, String componentId) {
        WindowedDataStream<Tuple2<K,V>> newDataStream = dataStream.mapWindow(new WindowMapFunction<T, Tuple2<K,V>>() {
            @Override
            public void mapWindow(Iterable<T> values, Collector<Tuple2<K,V>> collector) throws Exception {
                for(T value : values){
                    Tuple2<K,V> result = fun.mapPair(value);
                    collector.collect(result);
                }
            }
        });
        return new FlinkWindowedPairWorkloadOperator<>(newDataStream);
    }

    @Override
    public void print() {
        dataStream.flatten().print();
    }

}
