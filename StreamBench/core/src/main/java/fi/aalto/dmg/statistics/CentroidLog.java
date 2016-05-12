package fi.aalto.dmg.statistics;

import fi.aalto.dmg.util.Configure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * Created by jun on 22/03/16.
 */

public class CentroidLog implements Serializable {

    private static final long serialVersionUID = -2394729643285559L;
    private static Logger logger = LoggerFactory.getLogger(CentroidLog.class);


    public void execute(long counts, double[] location) {
        double probability = 0.05;
        if (Configure.kmeansCentroidsFrequency != null
                && Configure.kmeansCentroidsFrequency > 0) {
            probability = Configure.kmeansCentroidsFrequency;
        }

        if (Math.random() < probability) {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("\t%d", counts));
            for (double d : location) {
                sb.append(String.format("\t%16.14f", d));
            }
            logger.warn(sb.toString());
        }
    }
}

