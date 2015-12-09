package fi.aalto.dmg.frame.functions;

import fi.aalto.dmg.statistics.Latency;
import fi.aalto.dmg.util.WithTime;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 * Created by jun on 08/12/15.
 */
public class PairLatencySinkFunction<K,V> implements Function<Tuple2<K, V>, Boolean> {

    private final Logger logger = LoggerFactory.getLogger(PairLatencySinkFunction.class);
    private Latency latency;

    public PairLatencySinkFunction(){
        latency = new Latency(logger);
    }

    @Override
    public Boolean call(Tuple2<K, V> tuple2) throws Exception {
        latency.execute((WithTime)tuple2._2());
        return true;
    }
}
