package fi.aalto.dmg.frame;

import fi.aalto.dmg.frame.functions.GrouperPairFunctionImpl;
import fi.aalto.dmg.frame.functions.ReduceFunction;
import fi.aalto.dmg.util.TimeDurations;
import fi.aalto.dmg.util.Utils;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;

/**
 * Created by yangjun.wang on 25/10/15.
 */
public class SparkGroupedWorkloadOperator<K,V> implements GroupedWorkloadOperator<K,V> {

    private JavaPairDStream<K, Iterable<V>> pairDStream;

    public SparkGroupedWorkloadOperator(JavaPairDStream<K, Iterable<V>> stream){
        this.pairDStream = stream;
    }

    @Override
    public SparkPairWorkloadOperator<K, V> reduce(final ReduceFunction<V> fun, String componentId) {
        JavaPairDStream<K,V> newStream = this.pairDStream.mapToPair(new GrouperPairFunctionImpl<K,V>(fun));
        return new SparkPairWorkloadOperator<>(newStream);
    }

    @Override
    public WindowedPairWorkloadOperator<K, V> window(TimeDurations windowDuration) {
        return window(windowDuration, windowDuration);
    }

    @Override
    public WindowedPairWorkloadOperator<K, V> window(TimeDurations windowDuration, TimeDurations slideDuration) {
        Duration windowDurations = Utils.timeDurationsToSparkDuration(windowDuration);
        Duration slideDurations = Utils.timeDurationsToSparkDuration(slideDuration);
        JavaPairDStream<K, Iterable<V>> windowedStream = pairDStream.window(windowDurations, slideDurations);
        return new SparkWindowedPairWorkloadOperator<>(windowedStream);
    }
}
