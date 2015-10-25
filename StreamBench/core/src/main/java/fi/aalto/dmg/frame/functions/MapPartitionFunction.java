package fi.aalto.dmg.frame.functions;

import java.io.Serializable;

/**
 * Created by yangjun.wang on 25/10/15.
 */
public interface MapPartitionFunction<T, R> extends Serializable {
    Iterable<R> mapPartition(Iterable<T> var1);
}
