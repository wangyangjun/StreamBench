package fi.aalto.dmg.frame;

import fi.aalto.dmg.util.WithTime;

import java.io.Serializable;

/**
 * Created by yangjun.wang on 23/10/15.
 */
abstract public class OperatorCreator implements Serializable {
    private String appName;

    public String getAppName(){
        return this.appName;
    }

    public OperatorCreator(String name){
        this.appName = name;
    }

    /**
     * zkConStr: zoo1:2181
     * topics: Topic1,Topic2
     * offset smallest
     **/
    abstract public WorkloadOperator<WithTime<String>> createOperatorFromKafka(String zkConStr, String kafkaServers, String group, String topics, String offset);

    /**
     * Start streaming analysis job
     */
    abstract public void Start();

}
