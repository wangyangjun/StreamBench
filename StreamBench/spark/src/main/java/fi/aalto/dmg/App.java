package fi.aalto.dmg;

import fi.aalto.dmg.exceptions.WorkloadException;
import fi.aalto.dmg.workloads.WordCount;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws ClassNotFoundException, WorkloadException, InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException, IOException {
//        //BenchStarter.main(args);

        System.out.println("Start...");
        WordCount.main(args);
    }
}
