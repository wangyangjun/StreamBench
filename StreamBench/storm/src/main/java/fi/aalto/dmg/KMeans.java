package fi.aalto.dmg;

import fi.aalto.dmg.exceptions.WorkloadException;
import fi.aalto.dmg.frame.OperatorCreator;
import fi.aalto.dmg.frame.StormOperatorCreator;
import fi.aalto.dmg.workloads.WordCount;
import fi.aalto.dmg.workloads.Workload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.regex.Pattern;

/**
 * Created by jun on 28/02/16.
 */

public class KMeans
{
    public static void main( String[] args ) throws ClassNotFoundException, WorkloadException, InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException, IOException {

        Logger logger = LoggerFactory.getLogger("KMeans");
        logger.warn("Start ...");

        OperatorCreator operatorCreator = new StormOperatorCreator("KMeans");
        Workload workload = new fi.aalto.dmg.workloads.KMeans(operatorCreator);
        workload.Start();

//        BenchStarter.StartWorkload("KMeans");

    }
}

