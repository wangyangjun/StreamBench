package fi.aalto.dmg.frame.functions;

import com.google.common.base.Optional;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function2;

import java.util.List;

/**
 * Created by jun on 28/10/15.
 */
public class UpdateStateFunctionImpl<V> implements Function2<List<V>, Optional<V>, Optional<V>> {

    private static final long serialVersionUID = -7713561370480802413L;
    private static Logger logger = Logger.getLogger(ReduceFunctionImpl.class);

    private ReduceFunction<V> fun;

    public UpdateStateFunctionImpl(ReduceFunction<V> function){
        this.fun = function;
    }

    @Override
    public Optional<V> call(List<V> values, Optional<V> vOptional) throws Exception {
        V reducedValue = vOptional.orNull();
        for(V value : values) {
            if(null == reducedValue) {
                reducedValue = value;
            } else {
                reducedValue = fun.reduce(reducedValue, value);
            }
        }
        if(null != reducedValue)
            return Optional.of(reducedValue);
        return vOptional;
    }
}
