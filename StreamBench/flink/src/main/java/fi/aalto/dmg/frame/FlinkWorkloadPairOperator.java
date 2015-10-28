package fi.aalto.dmg.frame;

import fi.aalto.dmg.frame.functions.ReduceFunction;
import fi.aalto.dmg.frame.functions.UpdateStateFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.GroupedDataStream;
import org.apache.flink.util.Collector;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yangjun.wang on 24/10/15.
 */
public class FlinkWorkloadPairOperator<K,V> extends FlinkWorkloadOperator<Tuple2<K,V>> implements WorkloadPairOperator<K,V> {

    public FlinkWorkloadPairOperator(DataStream<Tuple2<K, V>> dataStream1) {
        super(dataStream1);
    }

    public FlinkWorkloadGrouperOperator<K, V> groupByKey(K key) {
        GroupedDataStream<Tuple2<K, V>> groupedDataStream = this.dataStream.groupBy(new KeySelector<Tuple2<K, V>, K>() {
            public K getKey(Tuple2<K, V> tuple2) throws Exception {
                return tuple2._1();
            }

        });
        return new FlinkWorkloadGrouperOperator<K, V>(groupedDataStream);
    }

    // TODO: reduceByKey - reduce first then groupByKey, at last reduce again
    public WorkloadPairOperator<K, V> reduceByKey(K key, final ReduceFunction<V> fun) {
//        DataStream<Tuple2<K,V>> tmpDataSet = this.dataStream.mapPartition(new MapPartitionFunction<Tuple2<K,V>, Tuple2<K,V>>() {
//            @Override
//            public void mapPartition(Iterable<Tuple2<K, V>> iterable, Collector<Tuple2<K,V>> collector) throws Exception {
//                HashMap<K, V> map = new HashMap<K, V>();
//                for(Tuple2<K, V> t : iterable){
//                    if(map.containsKey(t._1())){
//                        map.replace(t._1(), fun.reduce(map.get(t._1()),t._2()));
//                    } else {
//                        map.put(t._1(), t._2());
//                    }
//                }
//                for(Map.Entry<K, V> pair : map.entrySet()){
//                    collector.collect(new Tuple2<K, V>(pair.getKey(), pair.getValue()));
//                }
//            }
//        });
//        DataStream<Tuple2<K,V>> newDataStream = tmpDataSet.groupBy(new KeySelector<Tuple2<K, V>, K>() {
//            public K getKey(Tuple2<K, V> value) throws Exception {
//                return value._1();
//            }
//        }).reduce(new org.apache.flink.api.common.functions.ReduceFunction<Tuple2<K, V>>() {
//            public Tuple2<K, V> reduce(Tuple2<K, V> t1, Tuple2<K, V> t2) throws Exception {
//                return new Tuple2<K, V>(t1._1(), fun.reduce(t1._2(), t2._2()));
//            }
//        });
//
//        return new FlinkWorkloadPairOperator<K, V>(newDataStream);

        GroupedDataStream<Tuple2<K, V>> groupedDataStream = this.dataStream.groupBy(new KeySelector<Tuple2<K, V>, K>() {
            public K getKey(Tuple2<K, V> tuple2) throws Exception {
                return tuple2._1();
            }

        });
        DataStream<Tuple2<K,V>> newDataStream = groupedDataStream.reduce(new org.apache.flink.api.common.functions.ReduceFunction<Tuple2<K, V>>() {
            public Tuple2<K, V> reduce(Tuple2<K, V> t1, Tuple2<K, V> t2) throws Exception {
                return new Tuple2<K, V>(t1._1(), fun.reduce(t1._2(), t2._2()));
            }
        });
        return new FlinkWorkloadPairOperator<K, V>(newDataStream);
    }

    public WorkloadPairOperator<K, V> updateStateByKey(K key, UpdateStateFunction<V> fun) {
        return this;
    }
}

