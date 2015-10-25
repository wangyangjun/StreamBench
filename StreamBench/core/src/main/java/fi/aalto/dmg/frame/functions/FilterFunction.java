package fi.aalto.dmg.frame.functions;

import java.io.Serializable;

/**
 * Created by yangjun.wang on 23/10/15.
 */
public interface FilterFunction<T> extends Serializable {
    boolean filter(T var1) throws Exception;
}

