package fi.aalto.dmg;

import fi.aalto.dmg.exceptions.WorkloadException;
import fi.aalto.dmg.frame.OperatorCreator;
import fi.aalto.dmg.frame.StormOperatorCreator;
import fi.aalto.dmg.workloads.Workload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

/**
 * Created by jun on 12/04/16.
 */
public class AdvClick {
    public static void main( String[] args ) throws ClassNotFoundException, WorkloadException, InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException, IOException {

        Logger logger = LoggerFactory.getLogger("AdvClick");
        logger.warn("Start ...");

        OperatorCreator operatorCreator = new StormOperatorCreator("AdvClick");
        Workload workload = new fi.aalto.dmg.workloads.ClickedAdvertisement(operatorCreator);
        workload.Start();
    }
}
