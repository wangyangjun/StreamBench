package fi.aalto.dmg.generator;

import fi.aalto.dmg.statistics.Throughput;
import fi.aalto.dmg.util.Constant;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by jun on 28/02/16.
 */

public class KMeansPoints {
    private static final Logger logger = Logger.getLogger(SkewedWordCount.class);
    private static long POINT_NUM = 1000;
    private static String TOPIC = "KMeans";
    private static KafkaProducer<String, String> producer;


    public static List<Tuple2<Double, Double>> centroids = new ArrayList<>();

    public static void InitCentroids(){
        centroids.add(new Tuple2<>(0.0, 0.0));
        centroids.add(new Tuple2<>(0.0, 10.0));
        centroids.add(new Tuple2<>(0.0, -10.0));
        centroids.add(new Tuple2<>(10.0, 0.0));
        centroids.add(new Tuple2<>(10.0, 10.0));
        centroids.add(new Tuple2<>(10.0, -10.0));
        centroids.add(new Tuple2<>(-10.0, 0.0));
        centroids.add(new Tuple2<>(-10.0, 10.0));
        centroids.add(new Tuple2<>(-10.0, -10.0));

        centroids.add(new Tuple2<>(20.0, 0.0));
        centroids.add(new Tuple2<>(-20.0, 0.0));
        centroids.add(new Tuple2<>(0.0, 20.0));
        centroids.add(new Tuple2<>(0.0, -20.0));
        centroids.add(new Tuple2<>(10.0, -20.0));
        centroids.add(new Tuple2<>(10.0, 20.0));
        centroids.add(new Tuple2<>(-10.0, -20.0));
        centroids.add(new Tuple2<>(-10.0, 20.0));
        centroids.add(new Tuple2<>(20.0, -10.0));
        centroids.add(new Tuple2<>(20.0, -20.0));
        centroids.add(new Tuple2<>(20.0, 10.0));
        centroids.add(new Tuple2<>(20.0, 20.0));
        centroids.add(new Tuple2<>(-20.0, -10.0));
        centroids.add(new Tuple2<>(-20.0, -20.0));
        centroids.add(new Tuple2<>(-20.0, 10.0));
        centroids.add(new Tuple2<>(-20.0, 20.0));

        centroids.add(new Tuple2<>(0.0, 30.0));
        centroids.add(new Tuple2<>(0.0, -30.0));
        centroids.add(new Tuple2<>(30.0, 0.0));
        centroids.add(new Tuple2<>(-30.0, 0.0));
        centroids.add(new Tuple2<>(10.0, 30.0));
        centroids.add(new Tuple2<>(30.0, 10.0));
        centroids.add(new Tuple2<>(30.0, -10.0));

    }


    public static void main( String[] args ) throws InterruptedException {
        int SLEEP_FREQUENCY = 50;
        if(args.length > 0) {
            SLEEP_FREQUENCY = Integer.parseInt(args[0]);
        }

        if(null==producer){
            producer = Generator.createADVProducer();
        }
        InitCentroids();

        long time = System.currentTimeMillis();
        Throughput throughput = new Throughput("KMeansPoint");
        Random centroid_random = new Random(2342342170123L);
        Random point_random = new Random(1786238718324L);

        for(long generated_points=0; generated_points<POINT_NUM; generated_points++) {

            int centroid_index = centroid_random.nextInt(32);

            double x = centroids.get(centroid_index)._1() + point_random.nextDouble() * 10 - 5;
            double y = centroids.get(centroid_index)._2() + point_random.nextDouble() * 10 - 5;

            StringBuilder messageBuilder = new StringBuilder();
            messageBuilder.append(String.valueOf(x))
                    .append("\t")
                    .append(String.valueOf(y))
                    .append(Constant.TimeSeparator)
                    .append(String.valueOf(System.currentTimeMillis()));

            throughput.execute();
            ProducerRecord<String, String> newRecord = new ProducerRecord<String, String>(TOPIC, messageBuilder.toString());
            producer.send(newRecord);

            // control data generate speed
            if(generated_points%SLEEP_FREQUENCY == 0) {
                Thread.sleep(1);
            }
        }
        producer.close();
        logger.info("Latency: " + String.valueOf(System.currentTimeMillis()-time));

    }
}

