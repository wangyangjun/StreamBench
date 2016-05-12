package fi.aalto.dmg;

import fi.aalto.dmg.statistics.PerformanceStreamingListener;
import kafka.serializer.StringDecoder;
import org.apache.commons.math3.exception.DimensionMismatchException;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.mllib.clustering.StreamingKMeans;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Standalone application for Spark Streaming KMeans
 * Created by jun on 18/04/16.
 */
public class StreamKMeans {
    private static final Logger logger = Logger.getLogger(StreamKMeans.class);
    private static final int dimension = 2;

    private static class ParseKafkaString implements Function<Tuple2<String, String>, String> {

        @Override
        public String call(Tuple2<String, String> tuple2) {
            return tuple2._2();
        }
    }

    private static class ParsePoint implements Function<String, Vector> {
        private static final Pattern SPACE = Pattern.compile("\t");
        private static final String TimeSeparatorRegex = "\\|";

        @Override
        public Vector call(String str) {
            String[] list = str.split(TimeSeparatorRegex);
//            long time = System.currentTimeMillis();
//            if(list.length == 2) {
//                time = Long.parseLong(list[1]);
//            }
            String[] tok = SPACE.split(list[0]);
            double[] point = new double[tok.length];
            for (int i = 0; i < tok.length; ++i) {
                point[i] = Double.parseDouble(tok[i]);
            }
            return Vectors.dense(point);
        }
    }

    private static Vector[] loadInitCentroids() {
        List<Vector> centroids = new ArrayList<>();
        BufferedReader br = null;
        InputStream stream = null;
        try {
            String sCurrentLine;
            stream = StreamKMeans.class.getClassLoader().getResourceAsStream("init-centroids.txt");

            br = new BufferedReader(new InputStreamReader(stream));
            while ((sCurrentLine = br.readLine()) != null) {
                String[] strs = sCurrentLine.split(",");
                if (strs.length != dimension) {
                    throw new DimensionMismatchException(strs.length, dimension);
                }
                double[] position = new double[dimension];
                for (int i = 0; i < dimension; i++) {
                    position[i] = Double.valueOf(strs[i]);
                }
                centroids.add(Vectors.dense(position));
//                System.out.println(String.valueOf(x) + ", " + String.valueOf(y));
            }


        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (stream != null) stream.close();
                if (br != null) br.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        Vector[] vectors = new Vector[centroids.size()];
        for (int i = 0; i < centroids.size(); i++) {
            vectors[i] = centroids.get(i);
        }
        return vectors;
    }


    public static void main(String[] args) {

//        String inputFile = StreamKMeans.class.getClassLoader().getResource("centroids.txt").getFile();
        SparkConf sparkConf = new SparkConf().setMaster("spark://master:7077").setAppName("JavaKMeans");

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.milliseconds(1000));

        HashSet<String> topicsSet = new HashSet<>();
        topicsSet.add("KMeans");
        HashMap<String, String> kafkaParams = new HashMap<>();
//        kafkaParams.put("metadata.broker.list", "kafka1:9092,kafka2:9092,kafka3:9092");
        kafkaParams.put("metadata.broker.list", "localhost:9092");
        kafkaParams.put("auto.offset.reset", "largest");
        kafkaParams.put("zookeeper.connect", "zoo1:2181");
        kafkaParams.put("group.id", "spark");

        // Create direct kafka stream with brokers and topics
        JavaPairInputDStream<String, String> lines = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topicsSet
        );

        JavaDStream<Vector> points = lines.map(new ParseKafkaString()).map(new ParsePoint());

        Vector[] initCentroids = loadInitCentroids();
        double[] weights = new double[96];
        for (int i = 0; i < 96; i++) {
            weights[i] = 1.0 / 96;
        }

        final StreamingKMeans model = new StreamingKMeans()
                .setK(96)
                .setDecayFactor(0)
                .setInitialCenters(initCentroids, weights);

        model.trainOn(points);

        points.foreachRDD(new Function2<JavaRDD<Vector>, Time, Void>() {
            @Override
            public Void call(JavaRDD<Vector> vectorJavaRDD, Time time) throws Exception {
                Vector[] vector = model.latestModel().clusterCenters();
                for (int i = 0; i < vector.length; i++) {
                    logger.warn(vector[i].toArray()[0] + "\t" + vector[i].toArray()[1]);
                }
                return null;
            }
        });

        jssc.addStreamingListener(new PerformanceStreamingListener());
        jssc.start();
        jssc.awaitTermination();
    }
}
