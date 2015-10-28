package fi.aalto.dmg.frame.functions;

import org.apache.spark.api.java.function.*;

/**
 * Created by yangjun.wang on 28/10/15.
 */
public class FilterFunctionImpl<T> implements Function<T, Boolean> {
    private FilterFunction<T> fun;

    public FilterFunctionImpl(FilterFunction<T> function){
        fun = function;
    }

    @Override
    public Boolean call(T t) throws Exception {
        return fun.filter(t);
    }
}
