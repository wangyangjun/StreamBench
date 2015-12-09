package fi.aalto.dmg.statistics;

import org.apache.log4j.Logger;

import java.io.Serializable;

/**
 * Created by yangjun.wang on 14/10/15.
 * Measure the throughput when run out of stream tuple
 */
public class Throughput implements Serializable{

    private static final long serialVersionUID = -4968905648218161496L;
    private Logger logger;
    private long received;

    private long lastLogTime;
    private long lastLogEle;

    public Throughput(Logger logger) {
        this.logger = logger;
        this.received = 0;
        this.lastLogTime = 0;
        logger.warn("New throughput!!!");
    }

    public void execute(){
        long now = System.currentTimeMillis();
        received++;
        if(0 == lastLogTime) {
            this.lastLogTime = now;
        }
        long timeDiff = now - lastLogTime;
        if (timeDiff > 100) {
            long elementDiff = received - lastLogEle;
            double ex = (1000 / (double) timeDiff);

            logger.warn(String.format("Throughput:\t{}\t{}\t{}\tms,elements,elements/second",
                    timeDiff,
                    elementDiff,
                    Double.valueOf(elementDiff * ex).longValue()));
            // reinit
            lastLogEle = received;
            lastLogTime = now;
        }
    }
}
