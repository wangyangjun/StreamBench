package fi.aalto.dmg.frame;

import fi.aalto.dmg.frame.functions.*;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;

import java.util.Iterator;

/**
 * Created by jun on 11/3/15.
 */
public class SparkWindowedPairWorkloadOperator<K,V> implements WindowedPairWorkloadOperator<K,V> {

    private static final long serialVersionUID = 7216177060503270778L;
    private JavaPairDStream<K,V> pairDStream;

    public SparkWindowedPairWorkloadOperator(JavaPairDStream<K, V> stream) {
        this.pairDStream = stream;
    }

    @Override
    public PairWorkloadOperator<K, V> reduceByKey(ReduceFunction<V> fun, String componentId,
                                                  int parallelism,
                                                  boolean logThroughput) {
        return reduceByKey(fun, componentId, parallelism);
    }

    @Override
    public PairWorkloadOperator<K, V> reduceByKey(ReduceFunction<V> fun,
                                                  String componentId,
                                                  int parallelism) {
        JavaPairDStream<K,V> newStream = this.pairDStream.reduceByKey(new ReduceFunctionImpl<>(fun));
        return new SparkPairWorkloadOperator<>(newStream);
    }

    @Override
    public PairWorkloadOperator<K, V> updateStateByKey(ReduceFunction<V> fun,
                                                       String componentId,
                                                       int parallelism,
                                                       boolean logThroughput) {
        return updateStateByKey(fun, componentId, parallelism);
    }

    @Override
    public PairWorkloadOperator<K, V> updateStateByKey(ReduceFunction<V> fun,
                                                       String componentId,
                                                       int parallelism) {
        JavaPairDStream<K, V> cumulateStream = this.pairDStream.updateStateByKey(new UpdateStateFunctionImpl<>(fun));
        return new SparkPairWorkloadOperator<>(cumulateStream);
    }

    @Override
    public <R> PairWorkloadOperator<K, R> mapPartition(MapPartitionFunction<Tuple2<K, V>, Tuple2<K, R>> fun,
                                                       String componentId,
                                                       int parallelism,
                                                       boolean logThroughput) {
        return mapPartition(fun, componentId, parallelism);
    }

    @Override
    public <R> PairWorkloadOperator<K, R> mapPartition(MapPartitionFunction<Tuple2<K, V>, Tuple2<K, R>> fun,
                                                       String componentId,
                                                       int parallelism) {
        JavaPairDStream<K,R> newStream = pairDStream.mapPartitionsToPair(new PairMapPartitionFunctionImpl<>(fun));
        return new SparkPairWorkloadOperator<>(newStream);
    }

    @Override
    public <R> PairWorkloadOperator<K, R> mapValue(MapFunction<Tuple2<K, V>, Tuple2<K, R>> fun,
                                                   String componentId,
                                                   int parallelism,
                                                   boolean logThroughput) {
        return mapValue(fun, componentId, parallelism);
    }

    @Override
    public <R> PairWorkloadOperator<K, R> mapValue(MapFunction<Tuple2<K, V>, Tuple2<K, R>> fun,
                                                   String componentId,
                                                   int parallelism) {
        return null;
    }

    @Override
    public PairWorkloadOperator<K, V> filter(FilterFunction<Tuple2<K, V>> fun,
                                             String componentId,
                                             int parallelism,
                                             boolean logThroughput) {
        return filter(fun, componentId, parallelism);
    }

    @Override
    public PairWorkloadOperator<K, V> filter(FilterFunction<Tuple2<K, V>> fun,
                                             String componentId,
                                             int parallelism) {
        return null;
    }

    @Override
    public PairWorkloadOperator<K, V> reduce(ReduceFunction<Tuple2<K, V>> fun,
                                             String componentId,
                                             int parallelism,
                                             boolean logThroughput) {
        return reduce(fun, componentId, parallelism);
    }

    @Override
    public PairWorkloadOperator<K, V> reduce(ReduceFunction<Tuple2<K, V>> fun,
                                             String componentId,
                                             int parallelism) {
        return null;
    }

    @Override
    public void print() {
        pairDStream.print();
    }
}
