package fi.aalto.dmg.frame.functions;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function2;

/**
 * Created by yangjun.wang on 28/10/15.
 */
public class ReduceFunctionImpl<T> implements Function2<T, T, T> {

    private static final long serialVersionUID = 2239722821222828275L;
    private ReduceFunction<T> fun;
    private static Logger logger = Logger.getLogger(ReduceFunctionImpl.class);

    public ReduceFunctionImpl(ReduceFunction<T> function) {
        fun = function;
    }

    @Override
    public T call(T t1, T t2) throws Exception {
        return fun.reduce(t1, t2);
    }
}
