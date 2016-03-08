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

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by jun on 26/02/16.
 */
public class KMeans extends Workload implements Serializable {
    private static final long serialVersionUID = -7308411680185973067L;

    private static final Logger logger = Logger.getLogger(KMeans.class);

    public KMeans(OperatorCreator creator) throws WorkloadException {
        super(creator);
    }

    public static List<Point> InitCentroids(){
        List<Point> initCentroids = new ArrayList<>();
        initCentroids.add(new Point(0, 0.0, 0.0));
        initCentroids.add(new Point(1, 0.0, 10.0));
        initCentroids.add(new Point(2, 0.0, -10.0));
        initCentroids.add(new Point(3, 10.0, 0.0));
        initCentroids.add(new Point(4, 10.0, 10.0));
        initCentroids.add(new Point(5, 10.0, -10.0));
        initCentroids.add(new Point(6, -10.0, 0.0));
        initCentroids.add(new Point(7, -10.0, 10.0));
        initCentroids.add(new Point(8, -10.0, -10.0));

        initCentroids.add(new Point(9, 20.0, 0.0));
        initCentroids.add(new Point(10, -20.0, 0.0));
        initCentroids.add(new Point(11, 0.0, 20.0));
        initCentroids.add(new Point(12, 0.0, -20.0));
        initCentroids.add(new Point(13, 10.0, -20.0));
        initCentroids.add(new Point(14, 10.0, 20.0));
        initCentroids.add(new Point(15, -10.0, -20.0));
        initCentroids.add(new Point(16, -10.0, 20.0));
        initCentroids.add(new Point(17, 20.0, -10.0));
        initCentroids.add(new Point(18, 20.0, -20.0));
        initCentroids.add(new Point(19, 20.0, 10.0));
        initCentroids.add(new Point(20, 20.0, 20.0));
        initCentroids.add(new Point(21, -20.0, -10.0));
        initCentroids.add(new Point(22, -20.0, -20.0));
        initCentroids.add(new Point(23, -20.0, 10.0));
        initCentroids.add(new Point(24, -20.0, 20.0));

        initCentroids.add(new Point(25, 0.0, 30.0));
        initCentroids.add(new Point(26, 0.0, -30.0));
        initCentroids.add(new Point(27, 30.0, 0.0));
        initCentroids.add(new Point(28, -30.0, 0.0));
        initCentroids.add(new Point(29, 10.0, 30.0));
        initCentroids.add(new Point(30, 30.0, 10.0));
        initCentroids.add(new Point(31, 30.0, -10.0));

        return initCentroids;
    }

    @Override
    public void Process() throws WorkloadException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        try {
            WorkloadOperator<Point> points = getPointStream("source");
            points.iterative(); // points iteration

            List<Point> initCentroids = InitCentroids();
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
