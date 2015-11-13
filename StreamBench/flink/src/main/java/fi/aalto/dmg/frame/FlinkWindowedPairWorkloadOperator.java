package fi.aalto.dmg.frame;

import fi.aalto.dmg.frame.functions.*;
import fi.aalto.dmg.frame.functions.FilterFunction;
import fi.aalto.dmg.frame.functions.MapFunction;
import fi.aalto.dmg.frame.functions.MapPartitionFunction;
import fi.aalto.dmg.frame.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedDataStream;
import org.apache.flink.streaming.api.functions.WindowMapFunction;
import org.apache.flink.util.Collector;
import scala.Tuple2;
import com.google.common.base.Optional;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jun on 11/3/15.
 */
public class FlinkWindowedPairWorkloadOperator<K,V> implements WindowedPairWorkloadOperator<K,V>{

    protected WindowedDataStream<Tuple2<K,V>> dataStream;

    public FlinkWindowedPairWorkloadOperator(WindowedDataStream<Tuple2<K, V>> dataStream1) {
        this.dataStream = dataStream1;
    }

    @Override
    public WindowedPairWorkloadOperator<K, V> reduceByKey(final ReduceFunction<V> fun, String componentId) {
        WindowedDataStream<Tuple2<K,V>> newDataStream = this.dataStream.groupBy(new KeySelector<Tuple2<K, V>, K>() {
            public K getKey(Tuple2<K, V> tuple2) throws Exception {
                return tuple2._1();
            }
        }).reduceWindow(new org.apache.flink.api.common.functions.ReduceFunction<Tuple2<K, V>>() {
            @Override
            public Tuple2<K, V> reduce(Tuple2<K, V> t1, Tuple2<K, V> t2) throws Exception {
                return new Tuple2<>(t1._1(), fun.reduce(t1._2(), t2._2()));
            }
        });
        return new FlinkWindowedPairWorkloadOperator<>(newDataStream);
    }

    @Override
    public PairWorkloadOperator<K, V> updateStateByKey(final ReduceFunction<V> fun, String componentId) {
        DataStream<Tuple2<K,V>> newDataStream = this.dataStream.flatten().groupBy(new KeySelector<Tuple2<K, V>, K>() {
            public K getKey(Tuple2<K, V> tuple2) throws Exception {
                return tuple2._1();
            }
        }).reduce(new org.apache.flink.api.common.functions.ReduceFunction<Tuple2<K, V>>() {
            @Override
            public Tuple2<K, V> reduce(Tuple2<K, V> t0, Tuple2<K, V> t1) throws Exception {
                return new Tuple2<>(t0._1(), fun.reduce(t0._2(), t1._2()));
            }
        });
        return new FlinkPairWorkloadOperator<>(newDataStream);
    }

    @Override
    public <R> WindowedPairWorkloadOperator<K, R> mapPartition(final MapPartitionFunction<Tuple2<K, V>, Tuple2<K, R>> fun, String componentId) {
        WindowedDataStream<Tuple2<K,R>> newDataStream = dataStream.mapWindow(new WindowMapFunction<Tuple2<K,V>, Tuple2<K,R>>() {
            @Override
            public void mapWindow(Iterable<Tuple2<K, V>> values, Collector<Tuple2<K, R>> collector) throws Exception {
                Iterable<Tuple2<K,R>> results = fun.mapPartition(values);
                for (Tuple2<K,R> r : results) {
                    collector.collect(r);
                }
            }
        });
        return new FlinkWindowedPairWorkloadOperator<>(newDataStream);
    }

    @Override
    public <R> WindowedPairWorkloadOperator<K, R> mapValue(final MapFunction<Tuple2<K, V>, Tuple2<K, R>> fun, String componentId) {
        WindowedDataStream<Tuple2<K,R>> newDataStream = dataStream.mapWindow(new WindowMapFunction<Tuple2<K,V>, Tuple2<K,R>>() {
            @Override
            public void mapWindow(Iterable<Tuple2<K, V>> values, Collector<Tuple2<K, R>> collector) throws Exception {
                for(Tuple2<K, V> value : values){
                    Tuple2<K, R> result = fun.map(value);
                    collector.collect(result);
                }
            }
        });
        return new FlinkWindowedPairWorkloadOperator<>(newDataStream);
    }

    @Override
    public WindowedPairWorkloadOperator<K, V> filter(final FilterFunction<Tuple2<K, V>> fun, String componentId) {
        WindowedDataStream<Tuple2<K,V>> newDataStream = dataStream.mapWindow(new WindowMapFunction<Tuple2<K,V>, Tuple2<K,V>>() {
            @Override
            public void mapWindow(Iterable<Tuple2<K, V>> values, Collector<Tuple2<K, V>> collector) throws Exception {
                for(Tuple2<K, V> value : values){
                    if(fun.filter(value))
                        collector.collect(value);
                }
            }
        });
        return new FlinkWindowedPairWorkloadOperator<>(newDataStream);
    }

    @Override
    public WindowedPairWorkloadOperator<K, V> reduce(final ReduceFunction<Tuple2<K, V>> fun, String componentId) {
        WindowedDataStream<Tuple2<K,V>> newDataStream = dataStream.reduceWindow(new org.apache.flink.api.common.functions.ReduceFunction<Tuple2<K, V>>() {
            @Override
            public Tuple2<K, V> reduce(Tuple2<K, V> t1, Tuple2<K, V> t2) throws Exception {
                return fun.reduce(t1, t2);
            }
        });
        return new FlinkWindowedPairWorkloadOperator<>(newDataStream);
    }

    @Override
    public void print() {
        dataStream.flatten().print();
    }
}
