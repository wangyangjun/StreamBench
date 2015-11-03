package fi.aalto.dmg.frame;

import fi.aalto.dmg.frame.functions.ReduceFunction;
import fi.aalto.dmg.frame.functions.UpdateStateFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;

/**
 * Created by jun on 11/3/15.
 */
public class SparkWindowedPairWorkloadOperator<K,V> extends SparkWindowedWorkloadOperator<Tuple2<K,V>> implements WindowedPairWorkloadOperator<K,V> {

    private JavaPairDStream<K,V> pairDStream;

    public SparkWindowedPairWorkloadOperator(JavaPairDStream<K, V> stream) {
        super(stream.toJavaDStream());
        this.pairDStream = stream;
    }

    @Override
    public GroupedWorkloadOperator<K, V> groupByKey() {
        return null;
    }

    @Override
    public PairWorkloadOperator<K, V> reduceByKey(ReduceFunction<V> fun, String componentId) {
        return null;
    }

    @Override
    public PairWorkloadOperator<K, V> updateStateByKey(UpdateStateFunction<V> fun, String componentId) {
        return null;
    }
}
