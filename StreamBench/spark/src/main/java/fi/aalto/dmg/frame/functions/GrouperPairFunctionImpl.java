package fi.aalto.dmg.frame.functions;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Created by jun on 28/10/15.
 */
public class GrouperPairFunctionImpl<K,V> implements PairFunction<Tuple2<K,Iterable<V>>, K, V> {

    private static final long serialVersionUID = 5274813120639433080L;
    private ReduceFunction<V> fun;
    public GrouperPairFunctionImpl(ReduceFunction<V> function){
        this.fun = function;
    }

    @Override
    public Tuple2<K, V> call(Tuple2<K, Iterable<V>> kIterableTuple2) throws Exception {
        V reducedV = null;
        for(V v : kIterableTuple2._2()){
            if(null == reducedV){
                reducedV = v;
            }
            else {
                reducedV = fun.reduce(reducedV, v);
            }
        }
        return new Tuple2<>(kIterableTuple2._1(), reducedV);
    }
}
