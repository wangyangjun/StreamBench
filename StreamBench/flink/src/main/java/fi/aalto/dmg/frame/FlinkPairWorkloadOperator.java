package fi.aalto.dmg.frame;

import fi.aalto.dmg.frame.functions.*;
import fi.aalto.dmg.frame.functions.FilterFunction;
import fi.aalto.dmg.frame.functions.FlatMapFunction;
import fi.aalto.dmg.frame.functions.MapFunction;
import fi.aalto.dmg.frame.functions.ReduceFunction;
import fi.aalto.dmg.util.TimeDurations;
import fi.aalto.dmg.util.Utils;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.GroupedDataStream;
import org.apache.flink.streaming.api.datastream.WindowedDataStream;
import org.apache.flink.streaming.api.windowing.helper.Time;
import org.apache.flink.util.Collector;
import scala.Tuple2;

/**
 * Created by yangjun.wang on 24/10/15.
 */
public class FlinkPairWorkloadOperator<K,V> implements PairWorkloadOperator<K,V> {

    private DataStream<Tuple2<K, V>> dataStream;

    public FlinkPairWorkloadOperator(DataStream<Tuple2<K, V>> dataStream1) {
        this.dataStream = dataStream1;
    }

    public FlinkGroupedWorkloadOperator<K, V> groupByKey() {
        GroupedDataStream<Tuple2<K, V>> groupedDataStream = this.dataStream.groupBy(new KeySelector<Tuple2<K, V>, K>() {
            public K getKey(Tuple2<K, V> tuple2) throws Exception {
                return tuple2._1();
            }

        });
        return new FlinkGroupedWorkloadOperator<>(groupedDataStream);
    }

    // TODO: reduceByKey - reduce first then groupByKey, at last reduce again
    public PairWorkloadOperator<K, V> reduceByKey(final ReduceFunction<V> fun, String componentId) {
        GroupedDataStream<Tuple2<K, V>> groupedDataStream = this.dataStream.groupBy(new KeySelector<Tuple2<K, V>, K>() {
            public K getKey(Tuple2<K, V> tuple2) throws Exception {
                return tuple2._1();
            }
        });
        DataStream<Tuple2<K,V>> newDataStream = groupedDataStream.reduce(new org.apache.flink.api.common.functions.ReduceFunction<Tuple2<K, V>>() {
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
    public PairWorkloadOperator<K, V> filter(final FilterFunction<Tuple2<K, V>> fun, String componentId) {
        DataStream<Tuple2<K,V>> newDataStream = dataStream.filter(new org.apache.flink.api.common.functions.FilterFunction<Tuple2<K,V>>() {
            public boolean filter(Tuple2<K,V> t) throws Exception {
                return fun.filter(t);
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
    public PairWorkloadOperator<K, V> updateStateByKey(UpdateStateFunction<V> fun, String componentId) {
        return this;
    }

    @Override
    public WindowedPairWorkloadOperator<K, V> reduceByKeyAndWindow(ReduceFunction<V> fun, String componentId, TimeDurations windowDuration) {
        return reduceByKeyAndWindow(fun, componentId, windowDuration, windowDuration);
    }

    @Override
    public WindowedPairWorkloadOperator<K, V> reduceByKeyAndWindow(final ReduceFunction<V> fun, String componentId, TimeDurations windowDuration, TimeDurations slideDuration) {
        WindowedDataStream<Tuple2<K, V>> newDataStream = dataStream.groupBy(new KeySelector<Tuple2<K, V>, K>() {
            public K getKey(Tuple2<K, V> tuple2) throws Exception {
                return tuple2._1();
            }
        }).window(Time.of(windowDuration.getLength(), windowDuration.getUnit()))
                .every(Time.of(slideDuration.getLength(), slideDuration.getUnit()))
                .reduceWindow(new org.apache.flink.api.common.functions.ReduceFunction<Tuple2<K, V>>() {
                    @Override
                    public Tuple2<K, V> reduce(Tuple2<K, V> t1, Tuple2<K, V> t2) throws Exception {
                        V result = fun.reduce(t1._2(), t2._2());
                        return new Tuple2<>(t1._1(), result);
                    }
                });
        return new FlinkWindowedPairWorkloadOperator<>(newDataStream);
    }

    @Override
    public WindowedPairWorkloadOperator<K,V> window(TimeDurations windowDuration) {
        return window(windowDuration, windowDuration);
    }

    @Override
    public WindowedPairWorkloadOperator<K, V> window(TimeDurations windowDuration, TimeDurations slideDuration) {
        WindowedDataStream<Tuple2<K,V>> windowedDataStream = dataStream.window(Time.of(windowDuration.getLength(), windowDuration.getUnit()))
                .every(Time.of(slideDuration.getLength(), slideDuration.getUnit()));
        return new FlinkWindowedPairWorkloadOperator<>(windowedDataStream);
    }

    @Override
    public void print() {
        this.dataStream.print();
    }
}

