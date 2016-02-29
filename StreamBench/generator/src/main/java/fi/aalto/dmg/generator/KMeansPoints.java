package fi.aalto.dmg.generator;

import fi.aalto.dmg.statistics.Throughput;
import fi.aalto.dmg.util.Constant;
import fi.aalto.dmg.util.FastZipfGenerator;
import fi.aalto.dmg.util.Utils;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jun on 28/02/16.
 */

public class KMeansPoints {
    private static final Logger logger = Logger.getLogger(SkewedWordCount.class);
    private static long POINT_NUM = 1000000000;
    private static String TOPIC = "KMeans";
    private static KafkaProducer<String, String> producer;


    public static List<Tuple2<Double, Double>> points = new ArrayList<>();
    public static void InitPoints(){
        points.add(new Tuple2<>(-14.22, -48.01));
        points.add(new Tuple2<>(-22.78, -42.99));
        points.add(new Tuple2<>(56.18, -42.99));
        points.add(new Tuple2<>(35.04, 50.29));
        points.add(new Tuple2<>(-9.53, -46.26));
        points.add(new Tuple2<>(-34.35, 48.25));
        points.add(new Tuple2<>(55.82, -57.49));
        points.add(new Tuple2<>(21.03, 54.64));
        points.add(new Tuple2<>(-13.63, -42.26));
        points.add(new Tuple2<>(-36.57, 32.63));
        points.add(new Tuple2<>(50.65, -52.40));
        points.add(new Tuple2<>(24.48, 34.04));
        points.add(new Tuple2<>(-2.69, -36.02));
        points.add(new Tuple2<>(-38.80, 36.58));
        points.add(new Tuple2<>(24.00, -53.74));
        points.add(new Tuple2<>(32.41, 24.96));
        points.add(new Tuple2<>(-4.32, -56.92));
        points.add(new Tuple2<>(-22.68, 29.42));
        points.add(new Tuple2<>(59.02, -39.56));
        points.add(new Tuple2<>(24.47, 45.07));
        points.add(new Tuple2<>(5.23, -41.20));
        points.add(new Tuple2<>(-23.00, 38.15));
        points.add(new Tuple2<>(44.55, -51.50));
        points.add(new Tuple2<>(14.62, 59.06));
        points.add(new Tuple2<>(7.41, -56.05));
        points.add(new Tuple2<>(-26.63, 28.97));
        points.add(new Tuple2<>(47.37, -44.72));
        points.add(new Tuple2<>(29.07, 51.06));
        points.add(new Tuple2<>(0.59, -31.89));
        points.add(new Tuple2<>(-39.09, 20.78));
        points.add(new Tuple2<>(42.97, -48.98));
        points.add(new Tuple2<>(34.36, 49.08));
        points.add(new Tuple2<>(-21.91, -49.01));
        points.add(new Tuple2<>(-46.68, 46.04));
        points.add(new Tuple2<>(48.52, -43.67));
        points.add(new Tuple2<>(30.05, 49.25));
        points.add(new Tuple2<>(4.03, -43.56));
        points.add(new Tuple2<>(-37.85, 41.72));
        points.add(new Tuple2<>(38.24, -48.32));
        points.add(new Tuple2<>(20.83, 57.85));
    }


    public static void main( String[] args ) throws InterruptedException {
        int SLEEP_FREQUENCY = 50;
        if(args.length > 0) {
            SLEEP_FREQUENCY = Integer.parseInt(args[0]);
        }

        long time = System.currentTimeMillis();

        if(null==producer){
            producer = Generator.createADVProducer();
        }

        InitPoints();

        Throughput throughput = new Throughput("SkewedWordCount");
        // for loop to generate message
        for (Tuple2<Double, Double> point : points) {
            StringBuilder messageBuilder = new StringBuilder();

            // Add timestamp
            messageBuilder.append(point._1().toString())
                    .append("\t")
                    .append(point._2().toString())
                    .append(Constant.TimeSeparator)
                    .append(String.valueOf(time));

            throughput.execute();
            ProducerRecord<String, String> newRecord = new ProducerRecord<String, String>(TOPIC, messageBuilder.toString());
            producer.send(newRecord);
            System.out.println(messageBuilder.toString());
//            // control data generate speed
//            if(sent_sentences%SLEEP_FREQUENCY == 0) {
//                Thread.sleep(1);
//            }
        }
        producer.close();
        logger.info("Latency: " + String.valueOf(System.currentTimeMillis()-time));
    }
}

