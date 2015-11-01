package fi.aalto.dmg.frame;

import fi.aalto.dmg.frame.WindowedWordloadOperator;
import fi.aalto.dmg.frame.WorkloadOperator;
import fi.aalto.dmg.frame.functions.FunctionImpl;
import fi.aalto.dmg.frame.functions.MapPartitionFunction;
import fi.aalto.dmg.frame.functions.MapPartitionFunctionImpl;
import org.apache.spark.streaming.api.java.JavaDStream;

/**
 * Created by yangjun.wang on 31/10/15.
 */
public class SparkWindowedWorkloadOperator<T> implements WindowedWordloadOperator<T> {
    private JavaDStream<T> dStream;

    public SparkWindowedWorkloadOperator(JavaDStream<T> stream){
        dStream = stream;
    }


    @Override
    public <R> WorkloadOperator<R> mapPartition(MapPartitionFunction<T, R> fun, String componentId) {
        JavaDStream<R> newStream = dStream.mapPartitions(new MapPartitionFunctionImpl<>(fun));
        return new SparkWorkloadOperator<>(newStream);
    }
}
