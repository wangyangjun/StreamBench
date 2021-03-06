package fi.aalto.dmg.frame;

import fi.aalto.dmg.exceptions.UnsupportOperatorException;
import fi.aalto.dmg.frame.functions.GrouperPairFunctionImpl;
import fi.aalto.dmg.frame.functions.ReduceFunction;
import fi.aalto.dmg.util.TimeDurations;
import fi.aalto.dmg.util.Utils;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;

/**
 * Created by yangjun.wang on 25/10/15.
 */
public class SparkGroupedWorkloadOperator<K, V> extends GroupedWorkloadOperator<K, V> {

    private static final long serialVersionUID = 8638460461407972003L;
    private JavaPairDStream<K, Iterable<V>> pairDStream;

    public SparkGroupedWorkloadOperator(JavaPairDStream<K, Iterable<V>> stream, int parallelism) {
        super(parallelism);
        this.pairDStream = stream;
    }

    @Override
    public SparkPairWorkloadOperator<K, V> reduce(final ReduceFunction<V> fun, String componentId, int parallelism) {
        JavaPairDStream<K, V> newStream = this.pairDStream.mapToPair(new GrouperPairFunctionImpl<K, V>(fun));
        return new SparkPairWorkloadOperator<>(newStream, parallelism);
    }

    @Override
    public void closeWith(OperatorBase stream, boolean broadcast) throws UnsupportOperatorException {
        throw new UnsupportOperatorException("not implemented yet");
    }

    @Override
    public void print() {
    }
}
