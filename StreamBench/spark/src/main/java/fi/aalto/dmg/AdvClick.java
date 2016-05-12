package fi.aalto.dmg;

import fi.aalto.dmg.exceptions.WorkloadException;
import fi.aalto.dmg.frame.OperatorCreator;
import fi.aalto.dmg.frame.SparkOperatorCreater;
import fi.aalto.dmg.workloads.ClickedAdvertisement;
import fi.aalto.dmg.workloads.WordCount;
import fi.aalto.dmg.workloads.Workload;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.regex.Pattern;

/**
 * Created by jun on 01/02/16.
 */
public class AdvClick {
    public static void main(String[] args) throws ClassNotFoundException, WorkloadException, InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException, IOException {

        System.out.println("Start...");

        OperatorCreator operatorCreator = new SparkOperatorCreater("AdvClick");
        Workload workload = new ClickedAdvertisement(operatorCreator);
        workload.Start();

//        BenchStarter.StartWorkload("ClickedAdvertisement");
    }
}
