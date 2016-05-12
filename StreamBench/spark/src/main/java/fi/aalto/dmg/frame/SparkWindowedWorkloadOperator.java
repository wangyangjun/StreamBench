package fi.aalto.dmg.frame;

import fi.aalto.dmg.exceptions.UnsupportOperatorException;
import fi.aalto.dmg.frame.functions.*;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

/**
 * Created by yangjun.wang on 31/10/15.
 */
public class SparkWindowedWorkloadOperator<T> extends WindowedWorkloadOperator<T> {
    private static final long serialVersionUID = -6693062092516294209L;
    private JavaDStream<T> dStream;

    public SparkWindowedWorkloadOperator(JavaDStream<T> stream, int parallelism) {
        super(parallelism);
        dStream = stream;
    }

    @Override
    public <R> WorkloadOperator<R> mapPartition(MapPartitionFunction<T, R> fun,
                                                String componentId) {

        JavaDStream<R> newStream = dStream.mapPartitions(new MapPartitionFunctionImpl<>(fun));
        return new SparkWorkloadOperator<>(newStream, parallelism);
    }

    @Override
    public <R> WorkloadOperator<R> map(MapFunction<T, R> fun,
                                       String componentId) {
        JavaDStream<R> newStream = dStream.map(new FunctionImpl<>(fun));
        return new SparkWorkloadOperator<>(newStream, parallelism);
    }

    @Override
    public WorkloadOperator<T> filter(FilterFunction<T> fun,
                                      String componentId) {
        JavaDStream<T> newStream = dStream.filter(new FilterFunctionImpl<>(fun));
        return new SparkWorkloadOperator<>(newStream, parallelism);
    }

    @Override
    public WorkloadOperator<T> reduce(ReduceFunction<T> fun,
                                      String componentId) {
        JavaDStream<T> newStream = dStream.reduce(new ReduceFunctionImpl<>(fun));
        return new SparkWorkloadOperator<>(newStream, parallelism);
    }

    @Override
    public <K, V> PairWorkloadOperator<K, V> mapToPair(MapPairFunction<T, K, V> fun,
                                                       String componentId) {
        JavaPairDStream<K, V> pairDStream = dStream.mapToPair(new PairFunctionImpl<>(fun));
        return new SparkPairWorkloadOperator<>(pairDStream, parallelism);
    }

    @Override
    public void closeWith(OperatorBase stream, boolean broadcast) throws UnsupportOperatorException {
        throw new UnsupportOperatorException("don't support operator");
    }

    @Override
    public void print() {
        dStream.print();
    }
}
