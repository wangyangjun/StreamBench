package fi.aalto.dmg.frame;

import fi.aalto.dmg.frame.functions.ReduceFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;

/**
 * Created by yangjun.wang on 25/10/15.
 */
public class SparkWorkloadGrouperOperator<K,V> implements WorkloadGrouperOperator<K,V> {

    private JavaPairDStream<K, Iterable<V>> pairDStream;

    public SparkWorkloadGrouperOperator(JavaPairDStream<K, Iterable<V>> stream){
        this.pairDStream = stream;
    }

    @Override
    public SparkWorkloadPairOperator<K, V> reduce(final ReduceFunction<V> fun) {
        JavaPairDStream<K,V> newStream = this.pairDStream.mapToPair(new PairFunction<Tuple2<K,Iterable<V>>, K, V>() {
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
        });
        return new SparkWorkloadPairOperator<>(newStream);
    }
}
