package fi.aalto.dmg.frame.functions;

import com.google.common.base.Optional;
import fi.aalto.dmg.statistics.Latency;
import fi.aalto.dmg.util.WithTime;
import org.apache.spark.api.java.function.Function2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by jun on 28/10/15.
 */
public class UpdateStateFunctionImpl<V> implements Function2<List<V>, Optional<V>, Optional<V>> {

    private static final long serialVersionUID = -7713561370480802413L;
    private ReduceFunction<V> fun;
    private final Logger logger = LoggerFactory.getLogger(ReduceFunctionImpl.class);
    private Latency latency;

    public UpdateStateFunctionImpl(ReduceFunction<V> function){
        this.fun = function;
        latency = new Latency(logger);
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
            latency.execute((WithTime)reducedValue);
        }
        if(null != reducedValue)
            return Optional.of(reducedValue);
        return vOptional;
    }
}
