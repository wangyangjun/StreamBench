package fi.aalto.dmg;

import java.util.Iterator;

/**
 * Created by yangjun.wang on 25/10/15.
 */
public class Utils {
    public static <E> Iterable<E> iterable(final Iterator<E> iterator) {
        if (iterator == null) {
            throw new NullPointerException();
        }
        return new Iterable<E>() {
            public Iterator<E> iterator() {
                return iterator;
            }
        };
    }
}
