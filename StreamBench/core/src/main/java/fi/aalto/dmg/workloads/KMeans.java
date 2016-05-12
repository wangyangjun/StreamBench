package fi.aalto.dmg.workloads;

import fi.aalto.dmg.exceptions.WorkloadException;
import fi.aalto.dmg.frame.OperatorCreator;
import fi.aalto.dmg.frame.WorkloadOperator;
import fi.aalto.dmg.frame.userfunctions.UserFunctions;
import fi.aalto.dmg.util.Point;
import org.apache.commons.math3.exception.DimensionMismatchException;
import org.apache.log4j.Logger;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jun on 26/02/16.
 */
public class KMeans extends Workload implements Serializable {
    private static final long serialVersionUID = -7308411680185973067L;

    private static final Logger logger = Logger.getLogger(KMeans.class);

    private List<Point> initCentroids;
    private int centroidsNumber;
    private int dimension;

    public KMeans(OperatorCreator creator) throws WorkloadException {
        super(creator);
        centroidsNumber = Integer.parseInt(properties.getProperty("centroids.number"));
        dimension = Integer.parseInt(properties.getProperty("point.dimension", "2"));

        initCentroids = loadInitCentroids();
    }


    private List<Point> loadInitCentroids() {
        List<Point> centroids = new ArrayList<>();
        BufferedReader br = null;
        InputStream stream = null;
        try {
            String sCurrentLine;
            stream = this.getClass().getClassLoader().getResourceAsStream("init-centroids.txt");

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

    @Override
    public void Process() throws WorkloadException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        try {
            WorkloadOperator<Point> points = getPointStream("source");
            points.iterative(); // points iteration

            WorkloadOperator<Point> assigned_points = points.map(UserFunctions.assign, initCentroids, "assign", Point.class);
            WorkloadOperator<Point> centroids = assigned_points
                    .mapToPair(UserFunctions.pointMapToPair, "mapToPair")
                    .reduceByKey(UserFunctions.pointAggregator, "aggregator")
                    .map(UserFunctions.computeCentroid, "centroid", Point.class);
            points.closeWith(centroids, true);

            centroids.sink();
//            assigned_points.print();
//            points.print();

        } catch (Exception e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }
}
