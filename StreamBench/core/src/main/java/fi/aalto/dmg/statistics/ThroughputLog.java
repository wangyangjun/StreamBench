package fi.aalto.dmg.statistics;

import fi.aalto.dmg.util.Configure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * Created by yangjun.wang on 14/10/15.
 * Measure the throughput when run out of stream tuple
 */
public class ThroughputLog implements Serializable {

    private static final long serialVersionUID = -4968905648218161496L;
    private static Logger logger = LoggerFactory.getLogger(ThroughputLog.class);
    private String loggerName;
    private long received;

    private long lastLogTime;
    private long lastLogEle;

    public ThroughputLog(String loggerName) {
        this.loggerName = loggerName;
        this.received = 0;
        this.lastLogTime = 0;
    }

    public void execute() {
        if (Configure.throughputFrequency != null
                && Configure.throughputFrequency > 0) {
            execute(Configure.throughputFrequency);
        } else {
            execute(500);
        }
    }

    public void execute(int logFrequency) {
        long now = System.currentTimeMillis();
        received++;
        if (0 == lastLogTime) {
            this.lastLogTime = now;
        }
        long timeDiff = now - lastLogTime;
        if (timeDiff > logFrequency) {
            long elementDiff = received - lastLogEle;
            double ex = (1000 / (double) timeDiff);

            logger.warn(String.format(this.loggerName + ":\t%d\t%d\t%d\tms,elements,elements/second",
                    timeDiff,
                    elementDiff,
                    Double.valueOf(elementDiff * ex).longValue()));
            // reinit
            lastLogEle = received;
            lastLogTime = now;
        }
    }
}
