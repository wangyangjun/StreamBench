package fi.aalto.dmg.frame.functions;

import fi.aalto.dmg.statistics.Latency;
import fi.aalto.dmg.util.WithTime;
import org.apache.spark.api.java.function.Function2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yangjun.wang on 28/10/15.
 */
public class ReduceFunctionImpl<T> implements Function2<T,T,T>{

    private static final long serialVersionUID = 2239722821222828275L;
    private ReduceFunction<T> fun;
    private final Logger logger = LoggerFactory.getLogger(ReduceFunctionImpl.class);
    private Latency latency;

    public ReduceFunctionImpl(ReduceFunction<T> function){
        fun = function;
        latency = new Latency(logger);
    }

    @Override
    public T call(T t1, T t2) throws Exception {
        T t = fun.reduce(t1, t2);
//        latency.execute((WithTime)t);
        return t;
    }
}
