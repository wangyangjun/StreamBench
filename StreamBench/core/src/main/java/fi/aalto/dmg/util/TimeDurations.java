package fi.aalto.dmg.util;

import fi.aalto.dmg.exceptions.DurationException;

import java.util.concurrent.TimeUnit;

/**
 * A common window/slide time duration class
 * Created by jun on 11/3/15.
 */
public class TimeDurations {
    private TimeUnit unit;
    private long length;

    public TimeDurations(TimeUnit timeUnit, long timeLength) throws DurationException {
        switch (timeUnit) {
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

    public long toSeconds() {
        long seconds = this.length;
        switch (unit) {
            case MILLISECONDS:
                seconds = this.length / 1000L;
                break;
            case SECONDS:
                seconds = this.length;
                break;
            case MINUTES:
                seconds = this.length * 60L;
                break;
        }
        return seconds;
    }

    public long toMilliSeconds() {
        long milliseconds = this.length;
        switch (unit) {
            case MILLISECONDS:
                milliseconds = this.length;
                break;
            case SECONDS:
                milliseconds = this.length * 1000L;
                break;
            case MINUTES:
                milliseconds = this.length * 1000L * 60L;
                break;
        }
        return milliseconds;
    }

    public boolean equals(TimeDurations timeDurations) {
        return this.getLength() == timeDurations.getLength() && this.getUnit().equals(timeDurations.getUnit());
    }
}
