package fi.aalto.dmg.frame.functions;

import java.io.Serializable;

/**
 * Created by jun on 24/11/15.
 */
public interface AssignTimeFunction<T> extends Serializable {
    long assign(T var1);
}
