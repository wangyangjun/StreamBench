package fi.aalto.dmg.statistics;

import org.apache.log4j.Logger;

import java.io.Serializable;

/**
 * Created by yangjun.wang on 14/10/15.
 * Measure the throughput when run out of stream tuple
 */
public class Throughput implements Serializable{

    private static final long serialVersionUID = -4968905648218161496L;
    private static Logger logger;
    private long received;

    private long lastLogTime;
    private long lastLogEle;

    public Throughput(Logger log) {
        logger = log;
        this.received = 0;
        this.lastLogTime = 0;
    }

    public void execute(){
        execute(500);
    }

    public void execute(int logFrequency){
        long now = System.currentTimeMillis();
        received++;
        if(0 == lastLogTime) {
            this.lastLogTime = now;
        }
        long timeDiff = now - lastLogTime;
        if (timeDiff > logFrequency) {
            long elementDiff = received - lastLogEle;
            double ex = (1000 / (double) timeDiff);

            logger.warn(String.format("Throughput:\t%d\t%d\t%d\tms,elements,elements/second",
                    timeDiff,
                    elementDiff,
                    Double.valueOf(elementDiff * ex).longValue()));
            // reinit
            lastLogEle = received;
            lastLogTime = now;
        }
    }
}
