package fi.aalto.dmg.frame.functions;

import fi.aalto.dmg.statistics.ThroughputLog;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Created by yangjun.wang on 28/10/15.
 */
public class PairFunctionImpl<T,K,V> implements PairFunction<T,K,V>  {
    private static final long serialVersionUID = -1342161519291972356L;

    private MapPairFunction<T,K,V> fun;
    private ThroughputLog throughput;
    private boolean enableThroughput;

    public PairFunctionImpl(MapPairFunction<T,K,V> function){
        fun = function;
    }

    public PairFunctionImpl(MapPairFunction<T,K,V> function, boolean enableThroughput){
        this(function);
        this.enableThroughput = enableThroughput;
        throughput = new ThroughputLog(PairFunctionImpl.class.getSimpleName());
    }

    @Override
    public Tuple2<K, V> call(T t) throws Exception {
        if(enableThroughput) {
            throughput.execute();
        }
        return fun.mapToPair(t);
    }
}
