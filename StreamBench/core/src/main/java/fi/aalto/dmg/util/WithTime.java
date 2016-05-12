package fi.aalto.dmg.util;

import java.io.Serializable;

/**
 * Created by jun on 07/12/15.
 */
public class WithTime<T> implements Serializable {
    private T value;
    private long time;

    public WithTime(T v, long time) {
        this.value = v;
        this.time = time;
    }

    public WithTime(T v) {
        this.value = v;
        this.time = System.currentTimeMillis();
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    @Override
    public String toString() {
        return String.format("%s with time %d", value.toString(), time);
    }
}
