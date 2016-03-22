package fi.aalto.dmg.workloads;

import fi.aalto.dmg.exceptions.WorkloadException;
import fi.aalto.dmg.frame.OperatorCreator;
import fi.aalto.dmg.frame.PairWorkloadOperator;
import fi.aalto.dmg.frame.WorkloadOperator;
import fi.aalto.dmg.frame.functions.AssignTimeFunction;
import fi.aalto.dmg.frame.functions.MapFunction;
import fi.aalto.dmg.frame.functions.MapPairFunction;
import fi.aalto.dmg.frame.functions.ReduceFunction;
import fi.aalto.dmg.frame.userfunctions.UserFunctions;
import fi.aalto.dmg.util.Point;
import fi.aalto.dmg.util.TimeDurations;
import fi.aalto.dmg.util.WithTime;
import org.apache.log4j.Logger;
import scala.Tuple2;
import scala.Tuple3;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Created by jun on 26/02/16.
 */
public class KMeans extends Workload implements Serializable {
    private static final long serialVersionUID = -7308411680185973067L;

    private static final Logger logger = Logger.getLogger(KMeans.class);

    private List<Point> initCentroids = new ArrayList<>();
    private int centroidsNumber;

    public KMeans(OperatorCreator creator) throws WorkloadException {
        super(creator);
        centroidsNumber = Integer.parseInt(properties.getProperty("centroids.number"));
        loadCentroids();
    }

    // load real centroids which are used for data generation
    private void loadCentroids(){
        Random point_random = new Random(1786238718324L);
        for(int i=0; i<centroidsNumber; i++){
            double x = point_random.nextDouble() * 100 - 50;
            double y = point_random.nextDouble() * 100 - 50;
            initCentroids.add(new Point(i, x, y));
        }
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

        }
        catch (Exception e){
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }
}
