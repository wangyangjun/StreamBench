package fi.aalto.dmg.frame.functions;

import org.apache.spark.api.java.function.Function;

/**
 * Created by yangjun.wang on 04/11/15.
 */
public class FlatMapValuesFunctionImpl<V, R> implements Function<V, Iterable<R>> {

    private static final long serialVersionUID = 8493339432309258384L;
    FlatMapFunction<V, R> fun;

    public FlatMapValuesFunctionImpl(FlatMapFunction<V, R> function) {
        this.fun = function;
    }

    @Override
    public Iterable<R> call(V v) throws Exception {
        return fun.flatMap(v);
    }
}
