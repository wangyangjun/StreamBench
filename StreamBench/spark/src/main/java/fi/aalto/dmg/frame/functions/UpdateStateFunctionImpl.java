package fi.aalto.dmg.frame.functions;

import com.google.common.base.Optional;
import org.apache.spark.api.java.function.Function2;

import java.util.List;

/**
 * Created by jun on 28/10/15.
 */
public class UpdateStateFunctionImpl<V> implements Function2<List<V>, Optional<V>, Optional<V>> {

    private UpdateStateFunction<V> fun;
    public UpdateStateFunctionImpl(UpdateStateFunction<V> function){
        this.fun = function;
    }

    @Override
    public Optional<V> call(List<V> vs, Optional<V> vOptional) throws Exception {
        return fun.update(vs, vOptional);
    }
}
