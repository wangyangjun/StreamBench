package fi.aalto.dmg.util;

import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;

/**
 * Created by jun on 11/3/15.
 */
public class Utils {
    public static Duration timeDurationsToSparkDuration(TimeDurations timeDurations){
        Duration duration = Durations.seconds(1);
        switch(timeDurations.getUnit()){
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
}
