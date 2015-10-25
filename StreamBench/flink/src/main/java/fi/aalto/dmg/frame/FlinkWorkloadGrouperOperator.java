package fi.aalto.dmg.frame;

import fi.aalto.dmg.frame.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import scala.Tuple2;

/**
 * Created by yangjun.wang on 25/10/15.
 */
public class FlinkWorkloadGrouperOperator<K,V> implements WorkloadGrouperOperator<K,V> {
    private UnsortedGrouping<Tuple2<K,V>> grouper;

    public FlinkWorkloadGrouperOperator(UnsortedGrouping<Tuple2<K,V>> unsortedGrouping) {
        this.grouper = unsortedGrouping;
    }

    @Override
    public FlinkWorkloadPairOperator<K, V> reduce(final ReduceFunction<V> fun) {
        DataSet<Tuple2<K,V>> newDataSet = this.grouper.reduce(new org.apache.flink.api.common.functions.ReduceFunction<Tuple2<K,V>>() {
            public Tuple2<K,V> reduce(Tuple2<K,V> t1, Tuple2<K,V> t2) throws Exception {
                return new Tuple2<K, V>(t1._1(), fun.reduce(t1._2(), t2._2()));
            }
        });
        return new FlinkWorkloadPairOperator<K, V>(newDataSet);
    }
}
