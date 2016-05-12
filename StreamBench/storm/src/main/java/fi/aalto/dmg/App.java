package fi.aalto.dmg;

import fi.aalto.dmg.exceptions.WorkloadException;
import fi.aalto.dmg.frame.OperatorCreator;
import fi.aalto.dmg.frame.StormOperatorCreator;
import fi.aalto.dmg.workloads.WordCount;
import fi.aalto.dmg.workloads.WordCountWindowed;
import fi.aalto.dmg.workloads.Workload;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.regex.Pattern;

/**
 * Hello world!
 */
public class App {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws ClassNotFoundException, WorkloadException, InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException, IOException {

        System.out.println("Start ...");
        // WordCount WordCountWindowed FasterWordCount ClickedAdvertisement
        OperatorCreator operatorCreator = new StormOperatorCreator("WordCount");
        Workload workload = new WordCount(operatorCreator);
//        Workload workload = new WordCountWindowed(operatorCreator);
        workload.Start();

//        BenchStarter.StartWorkload("WordCount");

    }
}