package fi.aalto.dmg;

import fi.aalto.dmg.exceptions.WorkloadException;
import fi.aalto.dmg.frame.FlinkOperatorCreator;
import fi.aalto.dmg.frame.OperatorCreator;
import fi.aalto.dmg.workloads.WordCount;
import fi.aalto.dmg.workloads.Workload;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.regex.Pattern;

/**
 * Hello world!
 */
public class App {
    private static final Pattern SPACE = Pattern.compile(" ");
    private static Logger logger = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) throws ClassNotFoundException, WorkloadException, InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException, IOException {

        logger.warn("Start...");

        // WordCount WordCountWindowed FasterWordCount, ClickedAdvertisement
        OperatorCreator operatorCreator = new FlinkOperatorCreator("WordCount");
        Workload workload = new WordCount(operatorCreator);
        workload.Start();

//        BenchStarter.StartWorkload("WordCount");

    }
}
