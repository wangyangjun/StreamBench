package fi.aalto.dmg.generator;

import fi.aalto.dmg.statistics.Throughput;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.producer.KafkaProducer;

/**
 * Data generator for advertise click workload
 * Created by jun on 17/11/15.
 */
public class AdvertiseClickDataGenerator {
    private static final Logger logger = Logger.getLogger(WordCountDataGenerator.class);
    private static String ADV_TOPIC = "Advertisement";
    private static String CLICK_TOPIC = "AdvClick";
    private static double LAMBDA = 1;
    private static double CLICK_LAMBDA = 0.3;
    private static double CLICK_PROBABILITY = 0.01;

    private static int ADV_NUM = 1000000;
    private static KafkaProducer<String, String> producer;


    public static void main( String[] args ) throws InterruptedException {
        Throughput throughput = new Throughput("AdvDataGenerator");

        // Obtain a cached thread pool
        ExecutorService cachedPool = Executors.newFixedThreadPool(5000);

        RandomDataGenerator AdvertiseGenerator = new RandomDataGenerator();
        AdvertiseGenerator.reSeed(10000000L);
        // subthread use variable in main thread
        if(null==producer){
            producer = Generator.createProducer();
        }
        // for loop to generate advertisement
        for (int i = 0; i < ADV_NUM; ++i) {
//            double t = AdvertiseGenerator.nextExponential(LAMBDA);
//            Thread.sleep((long)(t*60*60));
            // advertisement id
            /*
            String advId = UUID.randomUUID().toString();
            long timestamp = System.currentTimeMillis();
            // TODO: write to kafka topic
            // write (t, advId) to kafka or wait t then write advId to kafka?
            producer.send(new ProducerRecord<String, String>(ADV_TOPIC, String.format("%d\t%s", timestamp, advId)));
//            System.out.println(timestamp + "\t" + advId);
            throughput.execute();
            // whether customer clicked this advertisement
            if(AdvertiseGenerator.nextUniform(0,1)<=CLICK_PROBABILITY){
                cachedPool.submit(new ClickThread(advId));
//                new ClickThread(advId).start();
            }
            */
            cachedPool.submit(new ClickThread(""));
            if(i%100 == 0) {
                System.out.println(i);
            }
        }

    }

    static class ClickThread implements Runnable{
        private String advId;
//        private Thread t;

        public ClickThread(String advId){
//            if (t == null) {
//                t = new Thread (this);
//            }
            this.advId = advId;
        }
        @Override
        public void run() {
            RandomDataGenerator clickGenerator = new RandomDataGenerator();

            double deltaT = clickGenerator.nextExponential(CLICK_LAMBDA);
            // TODO: write to kafka topic
            try {
                Thread.sleep((long)(deltaT*60*60));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            long timestamp = System.currentTimeMillis();
            producer.send(new ProducerRecord<String, String>(ADV_TOPIC, String.format("%d\t%s", timestamp, advId)));
//                System.out.println("Clicked: " + timestamp + "\t" + advId);
        }

//        public void start () {
//            t.start ();
//        }
    }
}
