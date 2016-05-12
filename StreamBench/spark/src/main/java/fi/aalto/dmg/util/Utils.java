package fi.aalto.dmg.util;

import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;

import java.util.Iterator;

/**
 * Created by jun on 11/3/15.
 */
public class Utils {
    public static Duration timeDurationsToSparkDuration(TimeDurations timeDurations) {
        Duration duration = Durations.seconds(1);
        switch (timeDurations.getUnit()) {
            case MILLISECONDS:
                duration = Durations.milliseconds(timeDurations.getLength());
                break;
            case SECONDS:
                duration = Durations.seconds(timeDurations.getLength());
                break;
            case MINUTES:
                duration = Durations.minutes(timeDurations.getLength());
                break;
        }
        return duration;
    }

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
