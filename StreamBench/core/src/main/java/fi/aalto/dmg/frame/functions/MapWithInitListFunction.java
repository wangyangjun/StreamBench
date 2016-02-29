package fi.aalto.dmg.frame.functions;

import java.io.Serializable;
import java.util.List;

/**
 * Created by jun on 27/02/16.
 */

public interface MapWithInitListFunction<T, R> extends Serializable {
    R map(T var1, List<T> list);
}
