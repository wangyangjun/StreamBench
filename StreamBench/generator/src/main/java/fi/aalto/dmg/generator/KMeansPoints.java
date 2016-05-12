package fi.aalto.dmg.generator;

import fi.aalto.dmg.statistics.ThroughputLog;
import fi.aalto.dmg.util.Constant;
import fi.aalto.dmg.util.Point;
import org.apache.commons.math3.distribution.MultivariateNormalDistribution;
import org.apache.commons.math3.exception.DimensionMismatchException;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Generator for Stream-KMeans workload
 * Created by jun on 28/02/16.
 */

public class KMeansPoints extends Generator {
    private static final Logger logger = Logger.getLogger(SkewedWordCount.class);
    private static long POINT_NUM = 100000000;
    private static String TOPIC;
    private static KafkaProducer<String, String> producer;

    private int dimension;
    private int centroidsNum;
    private double distance;

    private double[] means; // origin point
    private double[][] covariances;
    public List<Point> centroids;

    public KMeansPoints() throws Exception {
        super();
        producer = createBigBufferProducer();

        TOPIC = properties.getProperty("topic", "KMeans");

        dimension = Integer.parseInt(properties.getProperty("point.dimension", "2"));
        centroidsNum = Integer.parseInt(properties.getProperty("centroids.number", "96"));
        distance = Double.parseDouble(properties.getProperty("centroids.distance", "9"));

        means = new double[dimension];
        covariances = new double[dimension][dimension];
        String covariancesStr = properties.getProperty("covariances", "4,2,2,4");
        String[] covariancesStrs = covariancesStr.split(",");
        if (covariancesStrs.length != dimension * dimension) {
            throw new Exception("Incorrect covariances");
        }
        for (int i = 0; i < dimension; i++) {
            means[i] = 0;
            for (int j = 0; j < dimension; j++) {
                covariances[i][j] = Double.valueOf(covariancesStrs[i * dimension + j]);
            }
        }


    }

    // Generate 96 real centroids in [-50, 50] for both x and y dimensions
    private void GenerateCentroids() {
        Random random = new Random(12397238947287L);
        List<Point> centroids = new ArrayList<>();
        for (int i = 0; i < centroidsNum; ) {
            double[] position = new double[dimension];
            for (int j = 0; j < dimension; j++) {
                position[j] = random.nextDouble() * 100 - 50;
            }
            Point p = new Point(position);
            if (!centroids.contains(p)) {
                Point nearestP = null;
                double minDistance = Double.MAX_VALUE;
                for (Point centroid : centroids) {
                    double localDistance = p.distanceSquaredTo(centroid);
                    if (localDistance < minDistance) {
                        minDistance = localDistance;
                        nearestP = centroid;
                    }
                }
                if (nearestP == null || nearestP.distanceSquaredTo(p) > Math.pow(distance, 2)) {
                    centroids.add(p);
                    System.out.println(p.positonString());
                    i++;
                }
            }
        }

//        KDTree tree = new KDTree();
//        for(int i=0; i<centroidsNum; ){
//            double[] position = new double[dimension];
//            for(int j=0; j<dimension; j++){
//                position[i] = random.nextDouble()*100-50;
//            }
//            Point p = new Point(position);
//            if(!tree.contains(p)) {
//                Point nearestP = tree.nearest(p);
//                if(nearestP == null || nearestP.distanceSquaredTo(p) > Math.pow(distance, 2)) {
//                    tree.insert(p);
//                    System.out.println(p.positonString());
//                    i++;
//                }
//            }
//        }
    }

    private void GenerateInitCentroids() {
        centroids = loadCentroids();
        Random random = new Random(12397238947287L);
        List<Point> initCentroids = new ArrayList<>();
        RandomGenerator point_random = new JDKRandomGenerator();

        for (Point centroid : centroids) {
            MultivariateNormalDistribution distribution = new MultivariateNormalDistribution(point_random, means, covariances);

            double[] point = distribution.sample();
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < dimension - 1; i++) {
                point[i] += centroid.location[i];
                sb.append(point[i]).append(", ");
            }
            point[dimension - 1] += centroid.location[dimension - 1];
            sb.append(point[dimension - 1]);

            System.out.println(sb.toString());
        }
    }

    private List<Point> loadCentroids() {
        List<Point> centroids = new ArrayList<>();
        BufferedReader br = null;
        InputStream stream = null;
        try {
            String sCurrentLine;
            stream = KMeansPoints.class.getClassLoader().getResourceAsStream("centroids.txt");

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
                centroids.add(new Point(position));
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
        return centroids;
    }


    public void generate(int sleep_frequency) throws InterruptedException {
//        GenerateCentroids();
        centroids = loadCentroids();
        long time = System.currentTimeMillis();
        ThroughputLog throughput = new ThroughputLog(this.getClass().getSimpleName());
        Random centroid_random = new Random(2342342170123L);
        RandomGenerator point_random = new JDKRandomGenerator();
        point_random.setSeed(8624214);

        for (long generated_points = 0; generated_points < POINT_NUM; generated_points++) {

            int centroid_index = centroid_random.nextInt(centroids.size());
            MultivariateNormalDistribution distribution = new MultivariateNormalDistribution(point_random, means, covariances);

            double[] point = distribution.sample();
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < dimension - 1; i++) {
                point[i] += centroids.get(centroid_index).location[i];
                sb.append(point[i]).append("\t");
            }
            point[dimension - 1] += centroids.get(centroid_index).location[dimension - 1];
            sb.append(point[dimension - 1]).append(Constant.TimeSeparator).append(System.currentTimeMillis());

            throughput.execute();
            ProducerRecord<String, String> newRecord = new ProducerRecord<>(TOPIC, sb.toString());
            producer.send(newRecord);
//            System.out.println(sb.toString());
            // control data generate speed
            if (sleep_frequency > 0 && generated_points % sleep_frequency == 0) {
                Thread.sleep(1);
            }
        }

        producer.close();
        logger.info("LatencyLog: " + String.valueOf(System.currentTimeMillis() - time));
    }

    public static void main(String[] args) throws Exception {
        int SLEEP_FREQUENCY = -1;
        if (args.length > 0) {
            SLEEP_FREQUENCY = Integer.parseInt(args[0]);
        }

        // Generate real centroids
//        new KMeansPoints().GenerateCentroids();
        // Generate initialize centroids
//        new KMeansPoints().GenerateInitCentroids();
        // Generate points
        new KMeansPoints().generate(SLEEP_FREQUENCY);
    }
}

