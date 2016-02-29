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

    private static class TimeAssigner implements AssignTimeFunction<Long>, Serializable{
        @Override
        public long assign(Long var1) {
            return var1;
        }
    }

    @Override
    public void Process() throws WorkloadException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        try {
            WorkloadOperator<Point> points = getPointStream("source");

//            WorkloadOperator<WithTime<Point>> points = operator.map(UserFunctions.extractPoint, "extract");

            List<Point> initMeanList = new ArrayList<>();
            initMeanList.add(new Point(0, -31.85, -44.77));
            initMeanList.add(new Point(1, 35.16, 17.46));
            initMeanList.add(new Point(2, -5.16, 21.93));
            initMeanList.add(new Point(3, -24.06, 6.81));

            points.iterative();
            WorkloadOperator<Point> assigned_points = points.map(UserFunctions.assign, initMeanList, "assign", Point.class);
//            points iteration
            WorkloadOperator<Point> centroids = assigned_points
                    .mapToPair(UserFunctions.pointMapToPair, "keyGroup")
                    .reduceByKey(UserFunctions.pointAggregator, "aggregator")
                    .map(UserFunctions.computeCentroid, "centroid", Point.class);
            points.closeWith(centroids, true);

            centroids.print();
//            assigned_points.print();
//            points.print();

        }
        catch (Exception e){
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }
}
