package fi.aalto.dmg;

import fi.aalto.dmg.exceptions.WorkloadException;
import fi.aalto.dmg.frame.OperatorCreater;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Properties;

/**
 * Hello world!
 *
 */
public class BenchStarter
{
    public static final String OPERATOR_CREATER_CONFIGURE = "operator-creater.properties";
    public static final String CONFIGURE = "config.properties";

    private static final Logger logger = Logger.getLogger(BenchStarter.class);
    private static Properties config;
    private static Properties operatorCreatorConfig;

    public static void LoadConfigure() throws WorkloadException {
        config = new Properties();
        try {
            config.load(BenchStarter.class.getClassLoader().getResourceAsStream(CONFIGURE));
        } catch (IOException e) {
            logger.error("Read configure file " + CONFIGURE + " failed");
            throw new WorkloadException("Read configure file " + CONFIGURE + " failed");
        }

        operatorCreatorConfig = new Properties();
        try {
            operatorCreatorConfig.load(BenchStarter.class.getClassLoader().getResourceAsStream(OPERATOR_CREATER_CONFIGURE));
        } catch (IOException e) {
            logger.error("Read configure file " + OPERATOR_CREATER_CONFIGURE + " failed");
            throw new WorkloadException("Read configure file " + OPERATOR_CREATER_CONFIGURE + " failed");
        }
    }

    public static void main( String[] args ) throws WorkloadException {
        if(args.length < 1){
            logger.error("Usage: BenchStater workload");
            return;
        }
        logger.info("Start benchmark" );
        logger.info("Load configure files");
        LoadConfigure();

        ClassLoader classLoader = BenchStarter.class.getClassLoader();
        Class workloadClass;
        try {
            workloadClass = classLoader.loadClass("fi.aalto.dmg.workloads."+args[0]);
        } catch (ClassNotFoundException e) {
            logger.error("Workload not found!");
            return;
        }
        Workload workloadInstance;
        try {
            Class dbclass = classLoader.loadClass(operatorCreatorConfig.getProperty("operator.creator"));
            OperatorCreater operatorCreater = (OperatorCreater)dbclass.getConstructor(String.class).newInstance(args[0]);
            workloadInstance = (Workload)workloadClass.getConstructor(OperatorCreater.class).newInstance(operatorCreater);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Instance " + workloadClass + " failed");
            return;
        }
        workloadInstance.Start();

    }
}
