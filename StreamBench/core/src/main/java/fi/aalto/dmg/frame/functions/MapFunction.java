package fi.aalto.dmg.frame.functions;

import java.io.Serializable;

/**
 * Created by yangjun.wang on 21/10/15.
 */
public interface MapFunction<T, R> extends Serializable {
    R map(T var1);
}
