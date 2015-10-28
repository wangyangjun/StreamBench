package fi.aalto.dmg.frame.functions;

import com.google.common.base.Optional;

import java.io.Serializable;
import java.util.List;

/**
 * Created by yangjun.wang on 27/10/15.
 */
public interface UpdateStateFunction<T> extends Serializable {
    Optional<T> update(List<T> values, Optional<T> cumulateValue);
}

