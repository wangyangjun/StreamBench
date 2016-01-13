package fi.aalto.dmg;

import fi.aalto.dmg.exceptions.WorkloadException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.regex.Pattern;

/**
 * Hello world!
 *
 */
public class App
{
    private static final Pattern SPACE = Pattern.compile(" ");
    private static Logger logger = LoggerFactory.getLogger(App.class);
    public static void main( String[] args ) throws ClassNotFoundException, WorkloadException, InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException, IOException {

        logger.warn("Start...");
        String[] testArgs = {"ClickedAdvertisement"}; // WordCount WordCountWindowed FasterWordCount, ClickedAdvertisement
        BenchStarter.main(testArgs);

    }
}
