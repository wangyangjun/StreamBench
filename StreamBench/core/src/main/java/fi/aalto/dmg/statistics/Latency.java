package fi.aalto.dmg.statistics;

import fi.aalto.dmg.util.WithTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.Serializable;

/**
 * Created by jun on 08/12/15.
 */
public class Latency implements Serializable{

    private static final long serialVersionUID = -8124631262741665559L;
    private static Logger logger = LoggerFactory.getLogger(Latency.class);
    private String loggerName;

    public Latency(String loggerName) {
        this.loggerName = loggerName;
    }

    public void execute(WithTime<? extends Object> withTime){
        long latency = System.currentTimeMillis() - withTime.getTime();
        // probability to log 0.001
        if(Math.random() < 0.01) {
            logger.warn(String.format(this.loggerName + ":\t%d", latency));
        }
    }

    public void execute(long time){
        long latency = System.currentTimeMillis() - time;
        // probability to log 0.001
        if(Math.random() < 0.01) {
            logger.warn(String.format(this.loggerName + ":\t%d", latency));
        }
    }
}

