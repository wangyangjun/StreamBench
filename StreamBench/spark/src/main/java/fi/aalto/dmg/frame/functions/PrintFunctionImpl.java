package fi.aalto.dmg.frame.functions;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by jun on 11/4/15.
 */
public class PrintFunctionImpl<T> implements Function2<JavaRDD<T>, Time, Void> {

    private static final Logger logger = LoggerFactory.getLogger(PrintFunctionImpl.class);
    @Override
    public Void call(JavaRDD<T> tJavaRDD, Time time) throws Exception {

        List<T> list = tJavaRDD.take(10);
        // scalastyle:off println
        logger.warn("-------------------------------------------");
        logger.warn("Time: " + time);
        logger.warn("-------------------------------------------");
        for (T t : list) {
            logger.warn(t.toString());
        }
        logger.warn("\n");
        return null;
    }
}
