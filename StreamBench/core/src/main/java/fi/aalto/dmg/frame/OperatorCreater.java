package fi.aalto.dmg.frame;

/**
 * Created by yangjun.wang on 23/10/15.
 */
abstract public class OperatorCreater {
    private String appName;

    public String getAppName(){
        return this.appName;
    }

    public OperatorCreater(String name){
        this.appName = name;
    }

    /**
     * zkConStr: zoo1:2181
     * topics: Topic1,Topic2
     **/
    abstract public WorkloadOperator<String> createOperatorFromKafka(String zkConStr, String group, String topics);

    /**
     * Start streaming analysis job
     */
    abstract public void Start();

}
