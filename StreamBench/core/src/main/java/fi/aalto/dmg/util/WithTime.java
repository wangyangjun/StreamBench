package fi.aalto.dmg.util;

/**
 * Created by jun on 07/12/15.
 */
public class WithTime<T> {
    private T value;
    private long time;

    public WithTime(T v){
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
}
