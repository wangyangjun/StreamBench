package fi.aalto.dmg.frame;

import fi.aalto.dmg.exceptions.UnsupportOperatorException;
import fi.aalto.dmg.frame.functions.FilterFunction;
import fi.aalto.dmg.frame.functions.MapFunction;
import fi.aalto.dmg.frame.functions.MapPartitionFunction;
import fi.aalto.dmg.frame.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import scala.Tuple2;


/**
 * Created by jun on 11/3/15.
 */
public class FlinkWindowedPairWorkloadOperator<K,V, W extends Window> extends WindowedPairWorkloadOperator<K,V>{

    private static final long serialVersionUID = -6386272946145107837L;
    private WindowedStream<Tuple2<K, V>, K, W> windowStream;

    public FlinkWindowedPairWorkloadOperator(WindowedStream<Tuple2<K, V>, K, W> stream, int parallelism) {
        super(parallelism);
        windowStream = stream;
    }


    @Override
    public PairWorkloadOperator<K, V> reduceByKey(final ReduceFunction<V> fun, String componentId) {
        DataStream<Tuple2<K,V>> newDataStream = this.windowStream.reduce(new org.apache.flink.api.common.functions.ReduceFunction<Tuple2<K, V>>() {
            @Override
            public Tuple2<K, V> reduce(Tuple2<K, V> t1, Tuple2<K, V> t2) throws Exception {
                return new Tuple2<>(t1._1(), fun.reduce(t1._2(), t2._2()));
            }
        });
        return new FlinkPairWorkloadOperator<>(newDataStream, parallelism);
    }

    /**
     * In spark, updateStateByKey is used to accumulate data
     * windowedStream is already keyed in FlinkPairWorkload
     * @param fun user define funciton
     * @param componentId componment id
     * @return pair workload operator
     */
    @Override
    public PairWorkloadOperator<K, V> updateStateByKey(final ReduceFunction<V> fun, String componentId) {
        DataStream<Tuple2<K,V>> newDataStream = this.windowStream.reduce(new org.apache.flink.api.common.functions.ReduceFunction<Tuple2<K, V>>() {
            @Override
            public Tuple2<K, V> reduce(Tuple2<K, V> t1, Tuple2<K, V> t2) throws Exception {
                return new Tuple2<>(t1._1(), fun.reduce(t1._2(), t2._2()));
            }
        });
        return new FlinkPairWorkloadOperator<>(newDataStream, parallelism);
    }

    @Override
    public <R> PairWorkloadOperator<K, R> mapPartition(final MapPartitionFunction<scala.Tuple2<K, V>, scala.Tuple2<K, R>> fun, String componentId) {
        DataStream<Tuple2<K,R>> newDataStream = this.windowStream.apply(new WindowFunction<Tuple2<K, V>, Tuple2<K, R>, K, W>() {
            @Override
            public void apply(K k, W window, Iterable<Tuple2<K, V>> values, Collector<Tuple2<K, R>> collector) throws Exception {
                Iterable<Tuple2<K,R>> results = fun.mapPartition(values);
                for (Tuple2<K,R> r : results) {
                    collector.collect(r);
                }
            }
        });
        return new FlinkPairWorkloadOperator<>(newDataStream, parallelism);
    }

    @Override
    public <R> PairWorkloadOperator<K, R> mapValue(final MapFunction<Tuple2<K, V>, Tuple2<K, R>> fun, String componentId) {
        DataStream<Tuple2<K,R>> newDataStream = this.windowStream.apply(new WindowFunction<Tuple2<K, V>, Tuple2<K, R>, K, W>() {
            @Override
            public void apply(K k, W window, Iterable<Tuple2<K, V>> values, Collector<Tuple2<K, R>> collector) throws Exception {
                for(Tuple2<K, V> value : values){
                    Tuple2<K, R> result = fun.map(value);
                    collector.collect(result);
                }
            }
        });
        return new FlinkPairWorkloadOperator<>(newDataStream, parallelism);
    }

    @Override
    public PairWorkloadOperator<K, V> filter(final FilterFunction<Tuple2<K, V>> fun, String componentId) {
        DataStream<Tuple2<K,V>> newDataStream = this.windowStream.apply(new WindowFunction<Tuple2<K, V>, Tuple2<K, V>, K, W>() {
            @Override
            public void apply(K k, W window, Iterable<Tuple2<K, V>> values, Collector<Tuple2<K, V>> collector) throws Exception {
                for(Tuple2<K, V> value : values){
                    if(fun.filter(value))
                        collector.collect(value);
                }
            }
        });
        return new FlinkPairWorkloadOperator<>(newDataStream, parallelism);
    }

    @Override
    public PairWorkloadOperator<K, V> reduce(final ReduceFunction<Tuple2<K, V>> fun, String componentId) {
        DataStream<Tuple2<K, V>> newDataStream = this.windowStream.reduce(new org.apache.flink.api.common.functions.ReduceFunction<Tuple2<K, V>>() {
            @Override
            public Tuple2<K, V> reduce(Tuple2<K, V> t1, Tuple2<K, V> t2) throws Exception {
                return fun.reduce(t1, t2);
            }
        });
        return new FlinkPairWorkloadOperator<>(newDataStream, parallelism);
    }

    @Override
    public void closeWith(OperatorBase stream, boolean broadcast) throws UnsupportOperatorException {
        throw new UnsupportOperatorException("not implemented yet");
    }

    @Override
    public void print() {
        System.out.println("------------------------------");
    }
}
