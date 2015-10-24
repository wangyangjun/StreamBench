package fi.aalto.dmg.frame;

/**
 * Created by yangjun.wang on 23/10/15.
 */
abstract public class BuilderCreater<T> {
    /**
     * zkConStr: zoo1:2181
     * topics: Topic1,Topic2
     **/
    abstract public WorkloadOperator<T> createBuilderFromKafka(String zkConStr, String group, String topics);
}
