package fi.aalto.dmg.statistics;

import fi.aalto.dmg.util.WithTime;
import org.apache.log4j.Logger;

import java.io.Serializable;

/**
 * Created by jun on 08/12/15.
 */
public class Latency implements Serializable{

    private static final long serialVersionUID = -8124631262741665559L;
    private static Logger logger;

    public Latency(Logger log) {
        logger = log;
    }

    public void execute(WithTime<? extends Object> withTime){
        long latency = System.currentTimeMillis() - withTime.getTime();
        // probability to log 0.001
        logger.warn(String.format("Latency:\t%d", latency));
    }
}
