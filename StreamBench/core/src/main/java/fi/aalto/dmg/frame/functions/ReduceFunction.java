package fi.aalto.dmg.frame.functions;

import java.io.Serializable;

/**
 * Created by yangjun.wang on 21/10/15.
 */

public interface ReduceFunction<T> extends Serializable {
    T reduce(T var1, T var2) throws Exception;
}
