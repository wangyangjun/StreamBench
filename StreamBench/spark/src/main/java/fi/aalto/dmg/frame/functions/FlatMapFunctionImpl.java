package fi.aalto.dmg.frame.functions;

import org.apache.spark.api.java.function.FlatMapFunction;

/**
 * Created by yangjun.wang on 28/10/15.
 */
public class FlatMapFunctionImpl<T,R> implements FlatMapFunction<T, R> {
    fi.aalto.dmg.frame.functions.FlatMapFunction<T, R> fun;

    public FlatMapFunctionImpl(fi.aalto.dmg.frame.functions.FlatMapFunction<T,R> function){
        this.fun = function;
    }

    @Override
    public Iterable<R> call(T t) throws Exception {
        return fun.flatMap(t);
    }
}
