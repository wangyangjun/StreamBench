package fi.aalto.dmg.frame;

import fi.aalto.dmg.frame.functions.ReduceFunction;
import fi.aalto.dmg.util.TimeDurations;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.GroupedDataStream;
import org.apache.flink.streaming.api.datastream.WindowedDataStream;
import org.apache.flink.streaming.api.windowing.helper.Time;
import scala.Tuple2;

/**
 * Created by yangjun.wang on 25/10/15.
 */
public class FlinkGroupedWorkloadOperator<K,V> implements GroupedWorkloadOperator<K,V> {
    private GroupedDataStream<Tuple2<K,V>> groupedDataStream;

    public FlinkGroupedWorkloadOperator(GroupedDataStream<Tuple2<K, V>> groupedDataStream) {
        this.groupedDataStream = groupedDataStream;
    }

    public FlinkPairWorkloadOperator<K, V> reduce(final ReduceFunction<V> fun, String componentId) {

        DataStream<Tuple2<K,V>> newDataSet = this.groupedDataStream.reduce(new org.apache.flink.api.common.functions.ReduceFunction<Tuple2<K,V>>() {
            public Tuple2<K,V> reduce(Tuple2<K,V> t1, Tuple2<K,V> t2) throws Exception {
                return new Tuple2<>(t1._1(), fun.reduce(t1._2(), t2._2()));
            }
        });
        return new FlinkPairWorkloadOperator<>(newDataSet);
    }

    @Override
    public WindowedPairWorkloadOperator<K, V> window(TimeDurations windowDuration) {
        return window(windowDuration, windowDuration);
    }

    @Override
    public WindowedPairWorkloadOperator<K, V> window(TimeDurations windowDuration, TimeDurations slideDuration) {
        WindowedDataStream<Tuple2<K,V>> windowedDataStream = groupedDataStream.window(Time.of(windowDuration.getLength(), windowDuration.getUnit()))
                .every(Time.of(slideDuration.getLength(), slideDuration.getUnit()));
        return new FlinkWindowedPairWorkloadOperator<>(windowedDataStream);
    }
}
