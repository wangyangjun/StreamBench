package fi.aalto.dmg.util;

/**
 * Created by jun on 11/9/15.
 */
public class Utils {



    public static long getSeconds(TimeDurations durations){
        long seconds = 0L;
        switch (durations.getUnit()){
            case MILLISECONDS:
                seconds = durations.getLength()/1000L;
                break;
            case SECONDS:
                seconds = durations.getLength();
                break;
            case MINUTES:
                seconds = durations.getLength()*1000L;
                break;
        }
        return seconds;
    }
}
