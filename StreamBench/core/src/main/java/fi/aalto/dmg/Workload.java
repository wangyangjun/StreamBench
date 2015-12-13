package fi.aalto.dmg;

import fi.aalto.dmg.exceptions.WorkloadException;
import fi.aalto.dmg.frame.OperatorCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

/**
 * Created by yangjun.wang on 14/10/15.
 */
abstract public class Workload implements Serializable{
    private static final Logger logger = LoggerFactory.getLogger(Workload.class);

    private Properties properties;
    private OperatorCreator operatorCreator;

    public Workload(OperatorCreator creater) throws WorkloadException {
        this.operatorCreator = creater;
        properties = new Properties();
        String configFile = this.getClass().getSimpleName() + ".properties";
        try {
            properties.load(this.getClass().getClassLoader().getResourceAsStream(configFile));
        } catch (IOException e) {
            throw new WorkloadException("Read configure file " + configFile + " failed");
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Read configure file: " + configFile + " failed");
        }

    }

    protected Properties getProperties() {
        return properties;
    }
    protected OperatorCreator getOperatorCreator(){ return operatorCreator; }

    public void Start(){
        logger.info("Start workload: " + this.getClass().getSimpleName());
        try {
            Process();
            this.getOperatorCreator().Start();
        } catch (Exception e) {
            logger.error("WorkloadException caught when run workload " + this.getClass().getSimpleName());
            e.printStackTrace();
        }
        logger.info("The end of workload: " + this.getClass().getSimpleName());
    }

    abstract public void Process() throws WorkloadException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException;

}
