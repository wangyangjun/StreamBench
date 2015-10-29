package fi.aalto.dmg;

import fi.aalto.dmg.exceptions.WorkloadException;
import fi.aalto.dmg.frame.OperatorCreater;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

/**
 * Created by yangjun.wang on 14/10/15.
 */
abstract public class Workload implements Serializable{
    private final Logger logger;

    private String configFile;
    private Properties properties;
    private OperatorCreater operatorCreater;

    public Workload(OperatorCreater creater) throws WorkloadException {
        logger = Logger.getLogger(Workload.class);
        properties = new Properties();
        this.operatorCreater = creater;
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
    protected OperatorCreater getOperatorCreater(){ return operatorCreater; }

    public void Start(){
        logger.info("Start workload: " + this.getClass().getSimpleName());
        try {
            Process();
            this.getOperatorCreater().Start();
        } catch (Exception e) {
            logger.warn("WorkloadException caught when run workload " + this.getClass().getSimpleName());
            e.printStackTrace();
        }
        logger.info("The end of workload: " + this.getClass().getSimpleName());
    }

    abstract public void Process() throws WorkloadException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException;

}
