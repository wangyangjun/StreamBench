package fi.aalto.dmg;

import fi.aalto.dmg.exceptions.WorkloadException;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.regex.Pattern;

/**
 * Created by jun on 28/02/16.
 */

public class KMeans
{
    private static final Pattern SPACE = Pattern.compile(" ");
    public static void main( String[] args ) throws ClassNotFoundException, WorkloadException, InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException, IOException {

        System.out.println("Start ...");
        String[] testArgs = {"KMeans"};
        BenchStarter.main(testArgs);

    }
}

