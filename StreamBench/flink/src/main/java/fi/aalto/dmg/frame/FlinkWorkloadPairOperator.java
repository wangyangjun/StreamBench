package fi.aalto.dmg.frame;

import fi.aalto.dmg.frame.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import scala.Tuple2;

/**
 * Created by yangjun.wang on 24/10/15.
 */
public class FlinkWorkloadPairOperator<K,V> extends FlinkWorkloadOperator<Tuple2<K,V>> implements WorkloadPairOperator<K,V> {

    private UnsortedGrouping grouper;
    public FlinkWorkloadPairOperator(DataSet<Tuple2<K, V>> dataSet1) {
        super(dataSet1);
    }

    public FlinkWorkloadPairOperator(UnsortedGrouping unsortedGrouping) {
        super(null);
        this.grouper = unsortedGrouping;
    }


    public WorkloadPairOperator<K, Iterable<V>> groupByKey(K key) {
        UnsortedGrouping<Tuple2<K, V>> grouping = this.dataSet.groupBy(new KeySelector<Tuple2<K, V>, K>() {
            @Override
            public K getKey(Tuple2<K, V> tuple2) throws Exception {
                return tuple2._1();
            }
        });
        return new FlinkWorkloadPairOperator<K, Iterable<V>>(grouping);
    }

    // TODO: in spark, reduceByKey reduce first then groupByKey, at last reduce again
    public WorkloadPairOperator<K, V> reduceByKey(K key, final ReduceFunction<V> fun) {
        UnsortedGrouping<Tuple2<K, V>> grouping = this.dataSet.groupBy(new KeySelector<Tuple2<K, V>, K>() {
            @Override
            public K getKey(Tuple2<K, V> tuple2) throws Exception {
                return tuple2._1();
            }
        });
        DataSet<Tuple2<K,V>> newDataSet = grouping.reduce(new org.apache.flink.api.common.functions.ReduceFunction<Tuple2<K, V>>() {
            @Override
            public Tuple2<K, V> reduce(Tuple2<K, V> t1, Tuple2<K, V> t2) throws Exception {
                return new Tuple2<K, V>(t1._1(), fun.reduce(t1._2(), t2._2()));
            }
        });
        return new FlinkWorkloadPairOperator<K, V>(newDataSet);
    }
}
