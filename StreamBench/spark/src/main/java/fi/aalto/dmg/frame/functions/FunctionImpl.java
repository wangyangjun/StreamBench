package fi.aalto.dmg.frame.functions;

import org.apache.spark.api.java.function.*;

import java.io.Serializable;

/**
 * Created by yangjun.wang on 28/10/15.
 */
public class FunctionImpl<T,R> implements Function<T,R>, Serializable {
    private MapFunction<T,R> function;

    public FunctionImpl(MapFunction<T,R> fun){
        function = fun;
    }

    @Override
    public R call(T t) throws Exception {
        return function.map(t);
    }
}