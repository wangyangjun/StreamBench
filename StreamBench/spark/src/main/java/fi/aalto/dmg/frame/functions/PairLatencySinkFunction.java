package fi.aalto.dmg.frame.functions;

import fi.aalto.dmg.statistics.Latency;
import fi.aalto.dmg.util.WithTime;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

/**
 * Created by jun on 08/12/15.
 */
public class PairLatencySinkFunction<K,V> implements Function<Tuple2<K, V>, Boolean> {

    private static Logger logger = Logger.getLogger(PairLatencySinkFunction.class);
    private Latency latency;

    public PairLatencySinkFunction(){
        latency = new Latency(PairLatencySinkFunction.class.getSimpleName());
    }

    @Override
    public Boolean call(Tuple2<K, V> tuple2) throws Exception {
        latency.execute((WithTime)tuple2._2());
        return true;
    }
}
