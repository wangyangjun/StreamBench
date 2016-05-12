package fi.aalto.dmg;

import fi.aalto.dmg.exceptions.WorkloadException;
import fi.aalto.dmg.frame.OperatorCreator;
import fi.aalto.dmg.util.Configure;
import fi.aalto.dmg.workloads.Workload;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Properties;

/**
 * Benchmark starter
 * 1) load configuration for benchmark
 * 2) called from specific framework(storm, spark) projects
 */
public class BenchStarter {
    public static final String WORKLOAD_PACKAGE = "fi.aalto.dmg.workloads.";

    private static final Logger logger = Logger.getLogger(BenchStarter.class);

    public static void StartWorkload(String workload) throws WorkloadException {
        if (null == workload) {
            throw new WorkloadException("workload couldn't be null");
        }
        logger.info("Start benchmark workload: " + workload);
        Configure.LoadConfigure();

        ClassLoader classLoader = BenchStarter.class.getClassLoader();
        Class workloadClass;
        try {
            workloadClass = classLoader.loadClass(WORKLOAD_PACKAGE + workload);
        } catch (ClassNotFoundException e) {
            logger.error("Workload not found!");
            return;
        }

        Workload workloadInstance;
        try {
            Class operatorCreatorClass = classLoader.loadClass(Configure.operatorCreator);
            OperatorCreator operatorCreator = (OperatorCreator) operatorCreatorClass.getConstructor(String.class).newInstance(workload);
            workloadInstance = (Workload) workloadClass.getConstructor(OperatorCreator.class).newInstance(operatorCreator);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Instance " + workloadClass + " failed");
            return;
        }
        workloadInstance.Start();
    }
}
