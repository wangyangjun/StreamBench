package fi.aalto.dmg.workloads;

import fi.aalto.dmg.frame.WorkloadOperator;
import fi.aalto.dmg.frame.OperatorCreater;
import fi.aalto.dmg.frame.userfunctions.UserFunctions;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;

/**
 * Created by yangjun.wang on 27/10/15.
 */
public class WordCount implements Serializable{
    public static void main( String[] args ) throws ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        ClassLoader classLoader = WordCount.class.getClassLoader();
        Class dbclass = classLoader.loadClass("fi.aalto.dmg.frame.SparkOperatorCreater");
        OperatorCreater operatorCreater = (OperatorCreater)dbclass.getConstructor(String.class).newInstance("WordCount");
        WorkloadOperator<String> operator =
                operatorCreater.createOperatorFromKafka("localhost:2181", "asdf", "WordCount").map(UserFunctions.mapToSelf);
        operator.print();
        operatorCreater.Start();
    }

}
