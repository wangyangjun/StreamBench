package fi.aalto.dmg.generator;

import fi.aalto.dmg.statistics.Throughput;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Data generator for advertise click workload
 * Created by jun on 17/11/15.
 */
public class AdvClickDataGenerator {
    private static final Logger logger = Logger.getLogger(SkewedWordCount.class);
    private static String ADV_TOPIC = "Advertisement";
    private static String CLICK_TOPIC = "AdvClick";
    private static double LAMBDA = 1;
    private static double CLICK_LAMBDA = 0.3;
    private static double CLICK_PROBABILITY = 0.05;

    private static long ADV_NUM = 10000000;
    private static KafkaProducer<String, String> producer;


    public static void main( String[] args ) throws InterruptedException {
        int SLEEP_FREQUENCY = 1000;
        if(args.length > 0) {
            SLEEP_FREQUENCY = Integer.parseInt(args[0]);
        }

        Throughput throughput = new Throughput("AdvDataGenerator");

        // Obtain a cached thread pool
        ExecutorService cachedPool = Executors.newCachedThreadPool();

        RandomDataGenerator generator = new RandomDataGenerator();
        generator.reSeed(10000000L);
        // subthread use variable in main thread
        if(null==producer){
            producer = Generator.createProducer();
        }
        // for loop to generate advertisement
        for (long i = 0; i < 100; ++i) {
            // advertisement id
            String advId = UUID.randomUUID().toString();
            long timestamp = System.currentTimeMillis();
            // TODO: write to kafka topic
            // write (t, advId) to kafka or wait t then write advId to kafka?
            producer.send(new ProducerRecord<>(ADV_TOPIC, advId, String.format("%d\t%s", timestamp, advId)));
//            System.out.println(timestamp + "\t" + advId);
            throughput.execute();
            // whether customer clicked this advertisement
            if(generator.nextUniform(0,1)<=CLICK_PROBABILITY){
                cachedPool.submit(new ClickThread(advId));
            }

            // control data generate speed
            if(i%SLEEP_FREQUENCY == 0) {
                Thread.sleep(1);
            }
        }

        cachedPool.shutdown();
        try {
            cachedPool.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
        }

    }

    static class ClickThread implements Runnable{
        private String advId;

        public ClickThread(String advId){
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
            producer.send(new ProducerRecord<>(CLICK_TOPIC, advId,
                    String.format("%d\t%s", System.currentTimeMillis(), advId)));
//            System.out.println("Clicked: " + timestamp + "\t" + advId);
        }
    }
}
