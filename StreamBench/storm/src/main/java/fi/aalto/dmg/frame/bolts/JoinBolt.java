package fi.aalto.dmg.frame.bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import fi.aalto.dmg.util.TimeDurations;
import fi.aalto.dmg.util.Utils;
import scala.Tuple2;
import scala.Tuple3;

import java.util.LinkedList;

/**
 * Join two streams <K,V> <K,R> on K=K
 * emit Values(K, Tuple2<V,R>)
 * Created by jun on 17/11/15.
 */
public class JoinBolt<K,V,R> extends BaseBasicBolt {

    private String component1;
    private String component2;
    private long component1_window_seconds;
    private long component2_window_seconds;

    // time, key, value
    private LinkedList<Tuple3<Long, K, V>> dataContainer1;
    private LinkedList<Tuple3<Long, K,R>> dataContainer2;

    /**
     *
     * @param component1 component id of stream 1
     * @param windowDuration1 window duration of stream 1
     * @param component2 component id of stream 2
     * @param windowDuration2 window duration of stream 2
     */
    public JoinBolt(String component1, TimeDurations windowDuration1, String component2, TimeDurations windowDuration2){
        this.component1 = component1;
        this.component2 = component2;

        this.component1_window_seconds = Utils.getSeconds(windowDuration1);
        this.component2_window_seconds = Utils.getSeconds(windowDuration2);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        // get current time
        long currentTime = System.currentTimeMillis()/1000L;
        if(tuple.getSourceComponent().equals(this.component1)){
            K key = (K)tuple.getValue(0);
            V value = (V)tuple.getValue(1);
            // add at the end of the list
            dataContainer1.add(new Tuple3<Long, K, V>(currentTime, key, value));
            // join
            int expiredDataNum = 0;
            for(Tuple3<Long, K, R> element2 : dataContainer2){
                // clean expired data
                if(element2._1()+component2_window_seconds<currentTime){
                    expiredDataNum++;
                } else if( element2._2().equals(key)){
                    collector.emit(new Values(key, new Tuple2<V,R>(value, element2._3())));
                    return;
                }
            }
            // clean expired data
            for(int i=0; i<expiredDataNum; ++i){
                dataContainer2.removeFirst();
            }
        } else {
            K key = (K)tuple.getValue(0);
            R value = (R)tuple.getValue(1);
            dataContainer2.push(new Tuple3<Long, K, R>(currentTime, key, value));
            // join
            int expiredDataNum = 0;
            for(Tuple3<Long, K, V> element1 : dataContainer1){
                // clean expired data
                if(element1._1()+component1_window_seconds<currentTime){
                    expiredDataNum++;
                } else if( element1._2().equals(key)){
                    collector.emit(new Values(key, new Tuple2<V,R>(element1._3(), value)));
                    return;
                }
            }
            // clean expired data
            for(int i=0; i<expiredDataNum; ++i){
                dataContainer1.removeFirst();
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(BoltConstants.OutputKeyField, BoltConstants.OutputValueField));
    }
}
