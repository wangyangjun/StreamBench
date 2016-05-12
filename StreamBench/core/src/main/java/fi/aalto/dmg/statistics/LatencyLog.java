package fi.aalto.dmg.statistics;

import fi.aalto.dmg.util.Configure;
import fi.aalto.dmg.util.WithTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.Serializable;

/**
 * Created by jun on 08/12/15.
 */
public class LatencyLog implements Serializable {

    private static final long serialVersionUID = -8124631262741665559L;
    private static Logger logger = LoggerFactory.getLogger(LatencyLog.class);
    private String loggerName;

    public LatencyLog(String loggerName) {
        this.loggerName = loggerName;
    }

    public void execute(WithTime<? extends Object> withTime) {
        long latency = System.currentTimeMillis() - withTime.getTime();

        double frequency = 0.001;
        if (Configure.latencyFrequency != null
                && Configure.latencyFrequency > 0) {
            frequency = Configure.latencyFrequency;
        }

        // probability to log 0.001
        if (Math.random() < frequency) {
            logger.warn(String.format(this.loggerName + ":\t%d", latency));
        }
    }

    public void execute(long time) {
        long latency = System.currentTimeMillis() - time;
        double probability = 0.001;
        if (Configure.latencyFrequency != null
                && Configure.latencyFrequency > 0) {
            probability = Configure.latencyFrequency;
        }

        // probability to log 0.001
        if (Math.random() < probability) {
            logger.warn(String.format(this.loggerName + ":\t%d", latency));
        }
    }
}

