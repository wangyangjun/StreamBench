package fi.aalto.dmg.generator;

import fi.aalto.dmg.statistics.ThroughputLog;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Generator for Advertise Clicks workload
 * Created by jun on 28/01/16.
 */

public class AdvClick extends Generator {
    private static final Logger logger = Logger.getLogger(SkewedWordCount.class);
    private static String ADV_TOPIC;
    private static String CLICK_TOPIC;
    private double clickLambda;
    private double clickProbability;

    private static long ADV_NUM = 100000000;
    private static KafkaProducer<String, String> producer;

    public AdvClick() {
        super();
        producer = createBigBufferProducer();

        ADV_TOPIC = properties.getProperty("topic1", "Advertisement");
        CLICK_TOPIC = properties.getProperty("topic2", "AdvClick");

        clickProbability = Double.parseDouble(properties.getProperty("click.probability", "0.3"));
        clickLambda = Double.parseDouble(properties.getProperty("click.lambda", "10"));
    }

    public void generate(int sleep_frequency) throws InterruptedException {
        ThroughputLog throughput = new ThroughputLog(this.getClass().getSimpleName());
        long time = System.currentTimeMillis();

        // Obtain a cached thread pool
        ExecutorService cachedPool = Executors.newCachedThreadPool();

        RandomDataGenerator generator = new RandomDataGenerator();
        generator.reSeed(10000000L);
        // sub thread use variable in main thread
        // for loop to generate advertisement

        ArrayList<Advertisement> advList = new ArrayList<>();
        for (long i = 1; i < ADV_NUM; ++i) {
            // advertisement id
            String advId = UUID.randomUUID().toString();
            long timestamp = System.currentTimeMillis();
            producer.send(new ProducerRecord<>(ADV_TOPIC, advId, String.format("%d\t%s", timestamp, advId)));
//            System.out.println("Shown: " + System.currentTimeMillis() + "\t" + advId);

            // whether customer clicked this advertisement
            if (generator.nextUniform(0, 1) <= clickProbability) {
//                long deltaT = (long)generator.nextExponential(clickLambda)*1000;
                long deltaT = (long) generator.nextGaussian(clickLambda, 1) * 1000;
//                System.out.println(deltaT);
                advList.add(new Advertisement(advId, timestamp + deltaT));
            }

            if (i % 100 == 0) {
                cachedPool.submit(new ClickThread(advList));
                advList = new ArrayList<>();
            }

            throughput.execute();
            // control data generate speed
            if (sleep_frequency > 0 && i % sleep_frequency == 0) {
                Thread.sleep(1);
            }

        }
        cachedPool.shutdown();
        try {
            cachedPool.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
        }

        logger.info("LatencyLog: " + String.valueOf(System.currentTimeMillis() - time));

    }


    static class Advertisement implements Comparable<Advertisement> {
        Advertisement(String id, long time) {
            this.id = id;
            this.time = time;
        }

        String id;
        long time;

        @Override
        public int compareTo(Advertisement o) {
            if (this.time > o.time)
                return 1;
            else if (this.time == o.time)
                return 0;
            else
                return -1;
        }
    }

    static class ClickThread implements Runnable {
        private ArrayList<Advertisement> advList;

        public ClickThread(ArrayList<Advertisement> advList) {
            this.advList = advList;
            Collections.sort(this.advList);
        }

        @Override
        public void run() {
            for (Advertisement adv : advList) {
                if (System.currentTimeMillis() < adv.time) {
                    try {
                        Thread.sleep(adv.time - System.currentTimeMillis());
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                producer.send(new ProducerRecord<>(CLICK_TOPIC, adv.id,
                        String.format("%d\t%s", System.currentTimeMillis(), adv.id)));
//                System.out.println("Clicked: " + adv.id);
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        int SLEEP_FREQUENCY = -1;
        if (args.length > 0) {
            SLEEP_FREQUENCY = Integer.parseInt(args[0]);
        }
        new AdvClick().generate(SLEEP_FREQUENCY);
    }

}
