package fi.aalto.dmg.util;

import fi.aalto.dmg.exceptions.WorkloadException;
import fi.aalto.dmg.workloads.Workload;
import org.apache.commons.math3.util.DoubleArray;
import org.apache.log4j.Logger;
import scala.util.parsing.combinator.testing.Str;

import java.io.IOException;
import java.util.Properties;

/**
 * Configure class
 * load common configurations
 * <p>
 * Created by jun on 11/03/16.
 */
public class Configure {
    private static final Logger logger = Logger.getLogger(Configure.class);
    public static Properties config;

    public static final String COMMON_CONFIGURE = "config.properties";
    // configure properties
    private static final String OPERATOR_CREATOR = "operator.creator";
    private static final String THROUGHPUT_FREQUENCY = "throughput.frequency";
    private static final String LATENCY_FREQUENCY = "latency.frequency";
    private static final String KMEANS_CENTROIDS_FREQUENCY = "kmeans.centroids.frequency";
    private static final String CLUSTER_HOSTS = "cluster.hosts";
    private static final String HOST_CORES = "host.cores";

    public static String operatorCreator;
    public static Integer throughputFrequency; // milliseconds
    public static Double latencyFrequency; // probability
    public static Double kmeansCentroidsFrequency; // probability
    public static Integer clusterHosts;
    public static Integer hostCores;

    // load configuration from config.properties
    public static void LoadConfigure() throws WorkloadException {
        if (null == config) {
            config = new Properties();
            try {
                config.load(Configure.class.getClassLoader().getResourceAsStream(Configure.COMMON_CONFIGURE));
                // whether necessary configurations loaded
                operatorCreator = config.getProperty(OPERATOR_CREATOR);
                throughputFrequency = Integer.valueOf(config.getProperty(THROUGHPUT_FREQUENCY));
                latencyFrequency = Double.valueOf(config.getProperty(LATENCY_FREQUENCY));
                kmeansCentroidsFrequency = Double.valueOf(config.getProperty(KMEANS_CENTROIDS_FREQUENCY));
                clusterHosts = Integer.valueOf(config.getProperty(CLUSTER_HOSTS));
                hostCores = Integer.valueOf(config.getProperty(HOST_CORES));

                if (null == operatorCreator || null == throughputFrequency) {
                    throw new WorkloadException("Configuration missing");
                }
            } catch (IOException e) {
                logger.error("Read configure file " + Configure.COMMON_CONFIGURE + " failed");
                throw new WorkloadException("Read configure file " + Configure.COMMON_CONFIGURE + " failed");
            }
        }
    }

}
