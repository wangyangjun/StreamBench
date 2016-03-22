package fi.aalto.dmg.generator;

import fi.aalto.dmg.statistics.Throughput;
import fi.aalto.dmg.util.Constant;
import fi.aalto.dmg.util.KDTree;
import fi.aalto.dmg.util.Point;
import org.apache.commons.math3.distribution.MultivariateNormalDistribution;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import scala.Tuple2;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

/**
 * Created by jun on 28/02/16.
 */

public class KMeansPoints extends Generator {
    private static final Logger logger = Logger.getLogger(SkewedWordCount.class);
    private static long POINT_NUM = 100000000;
    private static String TOPIC;
    private static KafkaProducer<String, String> producer;

    private double[][] covariances = new double[2][2];
    public List<Tuple2<Double, Double>> centroids;

    public KMeansPoints() throws Exception {
        super();
        producer = createBigBufferProducer();
        centroids = loadCentroids();

        TOPIC = this.properties.getProperty("topic", "KMeans");

        String covariancesStr = this.properties.getProperty("covariances", "4,2,2,4");
        String[] covariancesStrs = covariancesStr.split(",");
        if(covariancesStrs.length != 4){
            throw new Exception("Incorrect covariances");
        }
        covariances[0][0] = Double.valueOf(covariancesStrs[0]);
        covariances[0][1] = Double.valueOf(covariancesStrs[1]);
        covariances[1][0] = Double.valueOf(covariancesStrs[2]);
        covariances[1][1] = Double.valueOf(covariancesStrs[3]);

    }

    // Generate 96 real centroids in [-50, 50] for both x and y dimensions
    private static void GenerateCentroids(){
        Random xRandom = new Random(7238947287L);
        Random yRandom = new Random(12761381734L);

        KDTree tree = new KDTree();
        for(int i=0; i<96; ){
            double x = xRandom.nextDouble()*100-50;
            double y = yRandom.nextDouble()*100-50;
            Point p = new Point(x, y);
            if(!tree.contains(p)) {
                Point nearestP = tree.nearest(p);
                if(nearestP == null || nearestP.distanceSquaredTo(p) > 81) {
                    tree.insert(new Point(x, y));
                    System.out.println(String.valueOf(x)
                            + ", " + String.valueOf(y));
                    i++;
                }
            }
        }
    }

    private List<Tuple2<Double, Double>> loadCentroids(){
        List<Tuple2<Double, Double>> centroids = new ArrayList<>();
        BufferedReader br = null;
        InputStream stream = null;
        try {
            String sCurrentLine;
            stream = KMeansPoints.class.getClassLoader().getResourceAsStream("centroids.txt");

            br = new BufferedReader(new InputStreamReader(stream));
            while ((sCurrentLine = br.readLine()) != null) {
                String[] strs = sCurrentLine.split(",");
                double x = Double.valueOf(strs[0]);
                double y = Double.valueOf(strs[1]);
                centroids.add(new Tuple2<>(x, y));
//                System.out.println(String.valueOf(x) + ", " + String.valueOf(y));
            }


        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (stream != null) stream.close();
                if (br != null)br.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        return centroids;
    }

    private static double[][] loadCovariances() throws Exception {
        Properties properties = new Properties();
        double[][] covariances = new double[2][2];
        try {
            properties.load(KMeansPoints.class.getClassLoader().getResourceAsStream("KMeansPoints.properties"));
            String covariancesStr = properties.getProperty("covariances");
            if(null == covariancesStr){
                throw new Exception("No covariances property found in KMeansPoints.properties");
            }
            String[] covariancesStrs = covariancesStr.split(",");
            if(covariancesStrs.length != 4){
                throw new Exception("Incorrect covariances");
            }
            covariances[0][0] = Double.valueOf(covariancesStrs[0]);
            covariances[0][1] = Double.valueOf(covariancesStrs[1]);
            covariances[1][0] = Double.valueOf(covariancesStrs[2]);
            covariances[1][1] = Double.valueOf(covariancesStrs[3]);

        } catch (IOException e) {
            e.printStackTrace();
        }
        return covariances;
    }

    public void generate(int sleep_frequency) throws InterruptedException {
//        GenerateCentroids();
        double[] means = new double[]{0, 0};

        long time = System.currentTimeMillis();
        Throughput throughput = new Throughput(this.getClass().getSimpleName());
        Random centroid_random = new Random(2342342170123L);
        RandomGenerator point_random = new JDKRandomGenerator();
        point_random.setSeed(8624214);

        for(long generated_points=0; generated_points<POINT_NUM; generated_points++) {

            int centroid_index = centroid_random.nextInt(centroids.size());
            MultivariateNormalDistribution distribution = new MultivariateNormalDistribution(point_random, means, covariances);

            double[] point = distribution.sample();

            double x = centroids.get(centroid_index)._1() + point[0];
            double y = centroids.get(centroid_index)._2() + point[1];

            String messageBuilder = String.valueOf(x) +
                    "\t" +
                    String.valueOf(y) +
                    Constant.TimeSeparator +
                    String.valueOf(System.currentTimeMillis());

            throughput.execute();
            ProducerRecord<String, String> newRecord = new ProducerRecord<String, String>(TOPIC, messageBuilder);
            producer.send(newRecord);
//            System.out.println(String.valueOf(x) + ", " + String.valueOf(y));
            // control data generate speed
            if(generated_points%sleep_frequency == 0) {
                Thread.sleep(1);
            }
        }

        producer.close();
        System.out.println("closed");
        logger.info("Latency: " + String.valueOf(System.currentTimeMillis()-time));
    }
    public static void main( String[] args ) throws Exception {
        int SLEEP_FREQUENCY = 50;
        if(args.length > 0) {
            SLEEP_FREQUENCY = Integer.parseInt(args[0]);
        }

//        GenerateCentroids();
        new KMeansPoints().generate(SLEEP_FREQUENCY);
    }
}

