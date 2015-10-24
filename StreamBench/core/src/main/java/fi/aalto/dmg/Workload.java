package fi.aalto.dmg;

import fi.aalto.dmg.exceptions.WorkloadException;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by yangjun.wang on 14/10/15.
 */
abstract public class Workload {
    private final Logger logger;

    private String configFile;
    private Properties properties;

    public Workload() throws WorkloadException {
        logger = Logger.getLogger(Workload.class);
        properties = new Properties();
    }

    public String getConfigFile() {
        return configFile;
    }

    public void setConfigFile(String configFile) throws WorkloadException {
        this.configFile = configFile;
        try {
            properties.load(this.getClass().getClassLoader().getResourceAsStream(configFile));
        } catch (IOException e) {
            throw new WorkloadException("Read configure file " + configFile + " failed");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected Properties getProperties() {
        return properties;
    }

    public void Start(){
        logger.info("Start workload: " + this.getClass().getSimpleName());
        try {
            Process();
        } catch (WorkloadException e) {
            logger.warn("WorkloadException caught when run workload " + this.getClass().getSimpleName());
            e.printStackTrace();
        }
        logger.info("The end of workload: " + this.getClass().getSimpleName());
    }

    abstract public void Process() throws WorkloadException;

}
