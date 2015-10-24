package fi.aalto.dmg;

import fi.aalto.dmg.exceptions.WorkloadException;
import fi.aalto.dmg.workloads.WordCountWorkload;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Properties;

/**
 * Hello world!
 *
 */
public class BenchStarter
{
    public static final String WORKLOADS_CONFIGURE = "workloads.properties";
    public static final String CONFIGURE = "config.properties";

    private static final Logger logger = Logger.getLogger(BenchStarter.class);
    private Properties config;
    private Properties workloads;

    public BenchStarter() throws WorkloadException {
        config = new Properties();
        try {
            config.load(this.getClass().getClassLoader().getResourceAsStream(CONFIGURE));
        } catch (IOException e) {
            logger.error("Read configure file " + CONFIGURE + " failed");
            throw new WorkloadException("Read configure file " + CONFIGURE + " failed");
        }

        workloads = new Properties();
        try {
            workloads.load(this.getClass().getClassLoader().getResourceAsStream(WORKLOADS_CONFIGURE));
        } catch (IOException e) {
            logger.error("Read configure file " + WORKLOADS_CONFIGURE + " failed");
            throw new WorkloadException("Read configure file " + WORKLOADS_CONFIGURE + " failed");
        }
    }

    public static void main( String[] args ) throws WorkloadException {
        BenchStarter benchStarter = new BenchStarter();

        logger.info("Start benchmark" );
        ClassLoader loader = BenchStarter.class.getClassLoader();
        String[] workloads = benchStarter.config.getProperty("stream.bench.workloads").split(",");
        for(String workload : workloads) {
            String workloadClass = benchStarter.workloads.getProperty(workload.trim());
            if( null == workloadClass)
                logger.warn(workload + " is not implemented, skip workload: " + workload);
            else{
                Class workerClass = null;
                try {
                    workerClass = loader.loadClass(workloadClass);
                } catch (ClassNotFoundException e) {
                    logger.warn(workload + " is not implemented, skip workload: " + workload);
                    continue;
                }

                Workload workloadInstance = null;
                try {
                    workloadInstance = (Workload)workerClass.newInstance();
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.error("Instance " + workloadClass + " failed");
                    continue;
                }
                workloadInstance.Start();
            }
        }
    }
}
