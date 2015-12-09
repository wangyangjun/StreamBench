package fi.aalto.dmg.statistics;

import fi.aalto.dmg.util.WithTime;
import org.apache.log4j.Logger;

import java.io.Serializable;

/**
 * Created by jun on 08/12/15.
 */
public class Latency implements Serializable{

    private static final long serialVersionUID = -8124631262741665559L;
    private Logger logger;
    private long received;

    private long lastLogTime;
    private long lastLogEle;
    private long acumulateLatency;

    public Latency(Logger logger) {
        this.logger = logger;
        this.received = 0;
        this.acumulateLatency = 0;
        this.lastLogTime = System.currentTimeMillis();
    }

    public void execute(WithTime<? extends Object> withTime){
        long now = System.currentTimeMillis();
        received++;
        acumulateLatency += now-withTime.getTime();
        long timeDiff = now - lastLogTime;
        long elementDiff = received - lastLogEle;
        if (timeDiff > 100 || elementDiff > 1000) {
            logger.warn(String.format("Latency:\t%d\t%d\t%d\tms,elements,ms/ele",
                    timeDiff, elementDiff, Double.valueOf(acumulateLatency/elementDiff).longValue()));
            // reinit
            lastLogEle = received;
            lastLogTime = now;
            acumulateLatency = 0;
        }
    }
}
