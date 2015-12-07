package fi.aalto.dmg.frame.functions;

import fi.aalto.dmg.statistics.Throughput;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 * Created by yangjun.wang on 28/10/15.
 */
public class PairFunctionImpl<T,K,V> implements PairFunction<T,K,V>  {
    private static final long serialVersionUID = -1342161519291972356L;
    private static final Logger logger = LoggerFactory.getLogger(PairFunctionImpl.class);

    private MapPairFunction<T,K,V> fun;
    Throughput throughput;

    public PairFunctionImpl(MapPairFunction<T,K,V> function){
        fun = function;
        throughput = new Throughput(logger);
    }

    @Override
    public Tuple2<K, V> call(T t) throws Exception {
        throughput.execute();
        return fun.mapToPair(t);
    }
}
