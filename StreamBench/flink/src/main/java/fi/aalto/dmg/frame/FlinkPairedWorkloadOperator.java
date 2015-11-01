package fi.aalto.dmg.frame;

import fi.aalto.dmg.frame.functions.ReduceFunction;
import fi.aalto.dmg.frame.functions.UpdateStateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.GroupedDataStream;
import scala.Tuple2;

/**
 * Created by yangjun.wang on 24/10/15.
 */
public class FlinkPairedWorkloadOperator<K,V> extends FlinkWorkloadOperator<Tuple2<K,V>> implements PairedWorkloadOperator<K,V> {

    public FlinkPairedWorkloadOperator(DataStream<Tuple2<K, V>> dataStream1) {
        super(dataStream1);
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
    public PairedWorkloadOperator<K, V> reduceByKey(final ReduceFunction<V> fun, String componentId) {
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
        return new FlinkPairedWorkloadOperator<>(newDataStream);
    }

    public PairedWorkloadOperator<K, V> updateStateByKey(UpdateStateFunction<V> fun, String componentId) {
        return this;
    }
}

