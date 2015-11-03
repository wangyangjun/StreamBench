package fi.aalto.dmg.workloads;

import fi.aalto.dmg.Workload;
import fi.aalto.dmg.exceptions.WorkloadException;
import fi.aalto.dmg.frame.OperatorCreater;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;

/**
 * Created by yangjun.wang on 03/11/15.
 */
public class WordCountWindowed  extends Workload implements Serializable {
    public WordCountWindowed(OperatorCreater creater) throws WorkloadException {
        super(creater);
    }

    @Override
    public void Process() throws WorkloadException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {

    }
}
