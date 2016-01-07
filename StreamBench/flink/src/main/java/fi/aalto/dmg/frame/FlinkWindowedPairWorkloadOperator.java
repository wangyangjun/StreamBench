package fi.aalto.dmg.frame;

import fi.aalto.dmg.frame.functions.*;
import fi.aalto.dmg.frame.functions.FilterFunction;
import fi.aalto.dmg.frame.functions.MapFunction;
import fi.aalto.dmg.frame.functions.MapPartitionFunction;
import fi.aalto.dmg.frame.functions.ReduceFunction;
import fi.aalto.dmg.statistics.Throughput;
import fi.aalto.dmg.util.Utils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import scala.Tuple2;


/**
 * Created by jun on 11/3/15.
 */
public class FlinkWindowedPairWorkloadOperator<K,V, W extends Window> implements WindowedPairWorkloadOperator<K,V>{

    private static final long serialVersionUID = -6386272946145107837L;
    private WindowedStream<Tuple2<K, V>, K, W> windowStream;

    public FlinkWindowedPairWorkloadOperator(WindowedStream<Tuple2<K, V>, K, W> stream) {
        windowStream = stream;
    }


    @Override
    public PairWorkloadOperator<K, V> reduceByKey(final ReduceFunction<V> fun, final String componentId, int parallelism, final boolean logThroughput) {
        DataStream<Tuple2<K,V>> newDataStream = this.windowStream.reduce(new org.apache.flink.api.common.functions.ReduceFunction<Tuple2<K, V>>() {
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

    @Override
    public PairWorkloadOperator<K, V> reduceByKey(final ReduceFunction<V> fun, String componentId, int parallelism) {
        return reduceByKey(fun, componentId, parallelism, false);
    }

    @Override
    public PairWorkloadOperator<K, V> updateStateByKey(final ReduceFunction<V> fun, final String componentId, int parallelism, final boolean logThroughput) {
        DataStream<Tuple2<K,V>> newDataStream = this.windowStream.reduce(new org.apache.flink.api.common.functions.ReduceFunction<Tuple2<K, V>>() {
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

    /**
     * In spark, updateStateByKey is used to accumulate data
     * windowedStream is already keyed in FlinkPairWorkload
     * @param fun
     * @param componentId
     * @return
     */
    @Override
    public PairWorkloadOperator<K, V> updateStateByKey(final ReduceFunction<V> fun, String componentId, int parallelism) {
        return updateStateByKey(fun, componentId, parallelism, false);
    }

    @Override
    public <R> PairWorkloadOperator<K, R> mapPartition(final MapPartitionFunction<Tuple2<K, V>, Tuple2<K, R>> fun, final String componentId, int parallelism, final boolean logThroughput) {
        DataStream<Tuple2<K,R>> newDataStream = this.windowStream.apply(new WindowFunction<Tuple2<K, V>, Tuple2<K, R>, K, W>() {
            Throughput throughput = new Throughput(componentId);

            @Override
            public void apply(K k, W window, Iterable<Tuple2<K, V>> values, Collector<Tuple2<K, R>> collector) throws Exception {

                Iterable<Tuple2<K,R>> results = fun.mapPartition(values);
                for (Tuple2<K,R> r : results) {
                    if(logThroughput) {
                        throughput.execute();
                    }
                    collector.collect(r);
                }
            }
        });
        return new FlinkPairWorkloadOperator<>(newDataStream);
    }

    @Override
    public <R> PairWorkloadOperator<K, R> mapPartition(final MapPartitionFunction<scala.Tuple2<K, V>, scala.Tuple2<K, R>> fun, String componentId, int parallelism) {
        return mapPartition(fun, componentId, parallelism, false);
    }

    @Override
    public <R> PairWorkloadOperator<K, R> mapValue(final MapFunction<Tuple2<K, V>, Tuple2<K, R>> fun, final String componentId, int parallelism, final boolean logThroughput) {
        DataStream<Tuple2<K,R>> newDataStream = this.windowStream.apply(new WindowFunction<Tuple2<K, V>, Tuple2<K, R>, K, W>() {
            Throughput throughput = new Throughput(componentId);

            @Override
            public void apply(K k, W window, Iterable<Tuple2<K, V>> values, Collector<Tuple2<K, R>> collector) throws Exception {
                for(Tuple2<K, V> value : values){
                    if(logThroughput) {
                        throughput.execute();
                    }
                    Tuple2<K, R> result = fun.map(value);
                    collector.collect(result);
                }
            }
        });
        return new FlinkPairWorkloadOperator<>(newDataStream);
    }

    @Override
    public <R> PairWorkloadOperator<K, R> mapValue(final MapFunction<Tuple2<K, V>, Tuple2<K, R>> fun, String componentId, int parallelism) {
        return mapValue(fun, componentId, parallelism, false);
    }

    @Override
    public PairWorkloadOperator<K, V> filter(final FilterFunction<Tuple2<K, V>> fun, final String componentId, int parallelism, final boolean logThroughput) {
        DataStream<Tuple2<K,V>> newDataStream = this.windowStream.apply(new WindowFunction<Tuple2<K, V>, Tuple2<K, V>, K, W>() {
            Throughput throughput = new Throughput(componentId);

            @Override
            public void apply(K k, W window, Iterable<Tuple2<K, V>> values, Collector<Tuple2<K, V>> collector) throws Exception {
                for(Tuple2<K, V> value : values){
                    if(logThroughput) {
                        throughput.execute();
                    }
                    if(fun.filter(value))
                        collector.collect(value);
                }
            }
        });
        return new FlinkPairWorkloadOperator<>(newDataStream);
    }

    @Override
    public PairWorkloadOperator<K, V> filter(final FilterFunction<Tuple2<K, V>> fun, String componentId, int parallelism) {
        return filter(fun, componentId, parallelism, false);
    }

    @Override
    public PairWorkloadOperator<K, V> reduce(final ReduceFunction<Tuple2<K, V>> fun, final String componentId, int parallelism, final boolean logThroughput) {
        DataStream<Tuple2<K, V>> newDataStream = this.windowStream.reduce(new org.apache.flink.api.common.functions.ReduceFunction<Tuple2<K, V>>() {
            Throughput throughput = new Throughput(componentId);

            @Override
            public Tuple2<K, V> reduce(Tuple2<K, V> t1, Tuple2<K, V> t2) throws Exception {
                if(logThroughput) {
                    throughput.execute();
                }
                return fun.reduce(t1, t2);
            }
        });
        return new FlinkPairWorkloadOperator<>(newDataStream);
    }

    @Override
    public PairWorkloadOperator<K, V> reduce(final ReduceFunction<Tuple2<K, V>> fun, String componentId, int parallelism) {
        return reduce(fun, componentId, parallelism, false);
    }

    @Override
    public void print() {
        System.out.println("------------------------------");
    }
}
