package fi.aalto.dmg.frame;

import fi.aalto.dmg.frame.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.util.Collector;

/**
 * Created by yangjun.wang on 24/10/15.
 */
public class FlinkWorkloadOperator<T> extends OperatorBase implements WorkloadOperator<T> {
    protected DataSet<T> dataSet;

    public FlinkWorkloadOperator(DataSet<T> dataSet1){
        dataSet = dataSet1;
    }

    public <R> WorkloadOperator<R> map(final MapFunction<T, R> fun) {
        DataSet<R> newDataSet = dataSet.map(new org.apache.flink.api.common.functions.MapFunction<T, R>() {
            public R map(T t) throws Exception {
                return fun.map(t);
            }
        });
        return new FlinkWorkloadOperator<R>(newDataSet);
    }

    public <K, V> WorkloadPairOperator<K, V> mapToPair(MapPairFunction<T, K, V> fun) {
        return null;
    }

    public WorkloadOperator<T> reduce(final ReduceFunction<T> fun) {
        DataSet<T> newDataSet = dataSet.reduce(new org.apache.flink.api.common.functions.ReduceFunction<T>() {
            public T reduce(T t, T t1) throws Exception {
                return fun.reduce(t, t1);
            }
        });
        return new FlinkWorkloadOperator<T>(newDataSet);
    }

    public WorkloadOperator<T> filter(final FilterFunction<T> fun) {
        DataSet<T> newDataSet = dataSet.filter(new org.apache.flink.api.common.functions.FilterFunction<T>() {
            public boolean filter(T t) throws Exception {
                return fun.filter(t);
            }
        });
        return new FlinkWorkloadOperator<T>(newDataSet);
    }

    public <R> WorkloadOperator<R> flatMap(final FlatMapFunction<T, R> fun) {
        DataSet<R> newDataSet = dataSet.flatMap(new org.apache.flink.api.common.functions.FlatMapFunction<T, R>() {
            public void flatMap(T t, Collector<R> collector) throws Exception {
                java.lang.Iterable<R> flatResults = fun.flatMap(t);
                for(R r : flatResults){
                    collector.collect(r);
                }
            }
        });
        return new FlinkWorkloadOperator<R>(newDataSet);
    }
}
