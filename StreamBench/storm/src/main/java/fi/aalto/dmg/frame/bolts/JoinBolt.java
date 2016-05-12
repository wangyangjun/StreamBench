package fi.aalto.dmg.frame.bolts;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import fi.aalto.dmg.frame.functions.AssignTimeFunction;
import fi.aalto.dmg.util.TimeDurations;
import org.apache.storm.shade.com.google.common.cache.Cache;
import org.apache.storm.shade.com.google.common.cache.CacheBuilder;
import org.apache.storm.shade.com.google.common.cache.CacheLoader;
import org.apache.storm.shade.com.google.common.cache.LoadingCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * In the benchmark, we use one side join
 * <p>
 * Join two streams <K,V> <K,R> on K=K
 * emit Values(K, Tuple2<V,R>)
 * Created by jun on 17/11/15.
 */
public class JoinBolt<K, V, R> extends BaseBasicBolt {
    private static final Logger logger = LoggerFactory.getLogger(JoinBolt.class);

    private static final long serialVersionUID = 4820980147212849642L;
    private String component1;
    private String component2;
    private long component1_window_milliseconds;
    private long component2_window_milliseconds;

    // time, key, value
//    private LinkedList<Tuple3<Long, K, V>> dataContainer1;
//    private LinkedList<Tuple3<Long, K, R>> dataContainer2;

    private Cache<K, Tuple4<Long, V, Long, R>> streamBuffer;

    // event time assigner
    private AssignTimeFunction<V> eventTimeAssigner1;
    private AssignTimeFunction<R> eventTimeAssigner2;

    /**
     * @param component1      component id of stream 1
     * @param windowDuration1 window duration of stream 1
     * @param component2      component id of stream 2
     * @param windowDuration2 window duration of stream 2
     */
    public JoinBolt(String component1, TimeDurations windowDuration1, String component2, TimeDurations windowDuration2) {
        this.component1 = component1;
        this.component2 = component2;

        this.component1_window_milliseconds = windowDuration1.toMilliSeconds();
        this.component2_window_milliseconds = windowDuration2.toMilliSeconds();

//        dataContainer1 = new LinkedList<>();
//        dataContainer2 = new LinkedList<>();
    }

    public JoinBolt(String component1, TimeDurations windowDuration1,
                    String component2, TimeDurations windowDuration2,
                    AssignTimeFunction<V> eventTimeAssigner1, AssignTimeFunction<R> eventTimeAssigner2) {
        this(component1, windowDuration1, component2, windowDuration2);
        this.eventTimeAssigner1 = eventTimeAssigner1;
        this.eventTimeAssigner2 = eventTimeAssigner2;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        long buffer_time = Math.max(component1_window_milliseconds, component2_window_milliseconds);

        streamBuffer = CacheBuilder.newBuilder()
                .expireAfterWrite(buffer_time, TimeUnit.MILLISECONDS)
                .build();

    }


    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
//        logger.warn("execute:" + tuple.toString());
        // get current time
        long currentTime = System.currentTimeMillis();
        K key = (K) tuple.getValue(0);

        if (tuple.getSourceComponent().equals(this.component1)) {
            V value = (V) tuple.getValue(1);
            if (null != this.eventTimeAssigner1) {
                currentTime = eventTimeAssigner1.assign(value);
            }

            Tuple4<Long, V, Long, R> tuple4 = streamBuffer.getIfPresent(key);
            if (null == tuple4) {
                streamBuffer.put(key, new Tuple4<Long, V, Long, R>(currentTime, value, null, null));
            } else {
                streamBuffer.invalidate(key);
                collector.emit(new Values(key, new Tuple2<V, R>(value, tuple4._4())));

                // check event time
//                if(currentTime <= tuple2._1() + component2_window_milliseconds) {
//                    collector.emit(new Values(key, new Tuple2<V, R>(value, tuple4._4())));
//                }
            }

        } else {
            R value = (R) tuple.getValue(1);
            if (null != this.eventTimeAssigner2) {
                currentTime = eventTimeAssigner2.assign(value);
            }

            Tuple4<Long, V, Long, R> tuple4 = streamBuffer.getIfPresent(key);
            if (null == tuple4) {
                streamBuffer.put(key, new Tuple4<Long, V, Long, R>(null, null, currentTime, value));
            } else {
                streamBuffer.invalidate(key);
                collector.emit(new Values(key, new Tuple2<V, R>(tuple4._2(), value)));

                // check event time
//                if(currentTime <= tuple2._1() + component1_window_milliseconds) {
//                  collector.emit(new Values(key, new Tuple2<V, R>(tuple4._2(), value)));
//                }

            }

        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(BoltConstants.OutputKeyField, BoltConstants.OutputValueField));
    }
}
