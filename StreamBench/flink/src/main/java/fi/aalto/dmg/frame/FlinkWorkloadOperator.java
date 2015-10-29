package fi.aalto.dmg.frame;

import fi.aalto.dmg.frame.functions.*;
import fi.aalto.dmg.frame.functions.FilterFunction;
import fi.aalto.dmg.frame.functions.FlatMapFunction;
import fi.aalto.dmg.frame.functions.MapFunction;
import fi.aalto.dmg.frame.functions.MapPartitionFunction;
import fi.aalto.dmg.frame.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.WindowMapFunction;
import org.apache.flink.util.Collector;

/**
 * Created by yangjun.wang on 24/10/15.
 */
public class FlinkWorkloadOperator<T> extends OperatorBase implements WorkloadOperator<T> {
    protected DataStream<T> dataStream;

    public FlinkWorkloadOperator(DataStream<T> dataSet1){
        dataStream = dataSet1;
    }

    public <R> WorkloadOperator<R> map(final MapFunction<T, R> fun) {
        DataStream<R> newDataSet = dataStream.map(new org.apache.flink.api.common.functions.MapFunction<T, R>() {
            public R map(T t) throws Exception {
                return fun.map(t);
            }
        });
        return new FlinkWorkloadOperator<R>(newDataSet);
    }

    public <R> WorkloadOperator<R> mapPartition(final MapPartitionFunction<T, R> fun) {
//        DataStream<R> newDataStream = dataStream.mapPartition(new org.apache.flink.api.common.functions.MapPartitionFunction<T, R>() {
//            @Override
//            public void mapPartition(Iterable<T> iterable, Collector<R> collector) throws Exception {
//                Iterable<R> results = fun.mapPartition(iterable);
//                for (R r : results) {
//                    collector.collect(r);
//                }
//            }
//        });
//        return new FlinkWorkloadOperator<R>(newDataStream);

        return null;
    }

    public <K, V> WorkloadPairOperator<K, V> mapToPair(MapPairFunction<T, K, V> fun) {
        return null;
    }

    public WorkloadOperator<T> reduce(final ReduceFunction<T> fun) {
        DataStream<T> newDataStream = dataStream.reduce(new org.apache.flink.api.common.functions.ReduceFunction<T>() {
            public T reduce(T t, T t1) throws Exception {
                return fun.reduce(t, t1);
            }
        });
        return new FlinkWorkloadOperator<T>(newDataStream);
    }

    public WorkloadOperator<T> filter(final FilterFunction<T> fun) {
        DataStream<T> newDataStream = dataStream.filter(new org.apache.flink.api.common.functions.FilterFunction<T>() {
            public boolean filter(T t) throws Exception {
                return fun.filter(t);
            }
        });
        return new FlinkWorkloadOperator<T>(newDataStream);
    }

    public <R> WorkloadOperator<R> flatMap(final FlatMapFunction<T, R> fun) {
        DataStream<R> newDataStream = dataStream.flatMap(new org.apache.flink.api.common.functions.FlatMapFunction<T, R>() {
            public void flatMap(T t, Collector<R> collector) throws Exception {
                java.lang.Iterable<R> flatResults = fun.flatMap(t);
                for(R r : flatResults){
                    collector.collect(r);
                }
            }
        });
        return new FlinkWorkloadOperator<R>(newDataStream);
    }

    public void print() {
        this.dataStream.print();
    }
}
