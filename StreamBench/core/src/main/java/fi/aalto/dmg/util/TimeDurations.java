package fi.aalto.dmg.util;

import fi.aalto.dmg.exceptions.DurationException;

import java.util.concurrent.TimeUnit;

/**
 * Created by jun on 11/3/15.
 */
public class TimeDurations {
    private TimeUnit unit;
    private long length;

    public TimeDurations(TimeUnit timeUnit, long timeLength) throws DurationException {
        switch(timeUnit){
            case MILLISECONDS:
                break;
            case SECONDS:
                break;
            case MINUTES:
                break;
            default:
                throw new DurationException("Unsupport time unit, please use millisecond, second or minute");
        }
        this.unit = timeUnit;
        this.length = timeLength;
    }

    public TimeUnit getUnit() {
        return unit;
    }

    public long getLength() {
        return length;
    }
}
