package fi.aalto.dmg;

import fi.aalto.dmg.exceptions.WorkloadException;
import fi.aalto.dmg.frame.FlinkOperatorCreator;
import fi.aalto.dmg.frame.OperatorCreator;
import fi.aalto.dmg.workloads.Workload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

/**
 * Created by jun on 29/03/16.
 */
public class WordCount {
    private static Logger logger = LoggerFactory.getLogger(WordCount.class);

    public static void main(String[] args) throws ClassNotFoundException, WorkloadException, InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException, IOException {
        logger.warn("Start...");

        OperatorCreator operatorCreator = new FlinkOperatorCreator("WordCount");
        Workload workload = new fi.aalto.dmg.workloads.WordCount(operatorCreator);
        workload.Start();

    }
}
