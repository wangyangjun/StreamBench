package fi.aalto.dmg.frame.functions;

import scala.Tuple2;

import java.io.Serializable;

/**
 * Created by yangjun.wang on 24/10/15.
 */
public interface MapPairFunction<T, K, V> extends Serializable {
    Tuple2<K, V> mapToPair(T t);
}
