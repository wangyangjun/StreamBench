package fi.aalto.dmg.frame;

import fi.aalto.dmg.frame.functions.*;
import fi.aalto.dmg.frame.functions.FilterFunction;
import fi.aalto.dmg.frame.functions.FlatMapFunction;
import fi.aalto.dmg.frame.functions.MapFunction;
import fi.aalto.dmg.frame.functions.ReduceFunction;
import fi.aalto.dmg.util.TimeDurations;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
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

    public <R> WorkloadOperator<R> map(final MapFunction<T, R> fun, String componentId) {
        DataStream<R> newDataStream = dataStream.map(new org.apache.flink.api.common.functions.MapFunction<T, R>() {
            public R map(T t) throws Exception {
                return fun.map(t);
            }
        });
        return new FlinkWorkloadOperator<>(newDataStream);
    }

    public <K, V> PairWorkloadOperator<K, V> mapToPair(final MapPairFunction<T, K, V> fun, String componentId) {
        DataStream<Tuple2<K,V>> newDataStream = dataStream.map(new org.apache.flink.api.common.functions.MapFunction<T, Tuple2<K, V>>() {
            public Tuple2<K, V> map(T t) throws Exception {
                scala.Tuple2<K,V> tuple2 = fun.mapPair(t);
                return new Tuple2<>(tuple2._1(), tuple2._2());
            }
        });
        return new FlinkPairWorkloadOperator<K,V>(newDataStream);
    }

    public WorkloadOperator<T> reduce(final ReduceFunction<T> fun, String componentId) {
        DataStream<T> newDataStream = dataStream.keyBy(0).reduce(new org.apache.flink.api.common.functions.ReduceFunction<T>() {
            public T reduce(T t, T t1) throws Exception {
                return fun.reduce(t, t1);
            }
        });
        return new FlinkWorkloadOperator<>(newDataStream);
    }

    public WorkloadOperator<T> filter(final FilterFunction<T> fun, String componentId) {
        DataStream<T> newDataStream = dataStream.filter(new org.apache.flink.api.common.functions.FilterFunction<T>() {
            public boolean filter(T t) throws Exception {
                return fun.filter(t);
            }
        });
        return new FlinkWorkloadOperator<>(newDataStream);
    }

    public <R> WorkloadOperator<R> flatMap(final FlatMapFunction<T, R> fun, String componentId) {
        TypeInformation<R> returnType = TypeExtractor.createTypeInfo(FlatMapFunction.class, fun.getClass(), 1, null, null);
        DataStream<R> newDataStream = dataStream.flatMap(new org.apache.flink.api.common.functions.FlatMapFunction<T, R>() {
            public void flatMap(T t, Collector<R> collector) throws Exception {
                java.lang.Iterable<R> flatResults = fun.flatMap(t);
                for(R r : flatResults){
                    collector.collect(r);
                }
            }
        }).returns(returnType);
        return new FlinkWorkloadOperator<>(newDataStream);
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
}
