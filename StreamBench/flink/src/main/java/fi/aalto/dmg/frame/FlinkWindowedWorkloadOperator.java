package fi.aalto.dmg.frame;

import fi.aalto.dmg.frame.functions.MapPartitionFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Created by yangjun.wang on 31/10/15.
 */
public class FlinkWindowedWorkloadOperator<T> implements WindowedWordloadOperator<T> {

    protected DataStream<T> dataStream;

    public FlinkWindowedWorkloadOperator(DataStream<T> dataSet1){
        dataStream = dataSet1;
    }

    public <R> WorkloadOperator<R> mapPartition(final MapPartitionFunction<T, R> fun, String componentId) {
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

}
