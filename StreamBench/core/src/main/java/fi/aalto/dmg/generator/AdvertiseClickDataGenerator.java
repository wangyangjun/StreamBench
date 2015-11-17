package fi.aalto.dmg.generator;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.log4j.Logger;

import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.producer.KafkaProducer;

/**
 * Created by jun on 17/11/15.
 */
public class AdvertiseClickDataGenerator {
    private static final Logger logger = Logger.getLogger(WordCountDataGenerator.class);
    private static String ADV_TOPIC = "Advertisement";
    private static String CLICK_TOPIC = "AdvClick";
    private static double LAMBDA = 1;
    private static double CLICK_LAMBDA = 0.33;
    private static int SENTENCE_NUM = 100;
    private static double CLICK_PROBABILITY = 0.33;
    private static int ZIPF_SIZE = 100000;
    private static long ZIPF_EXPONENT = 1L;
    private static KafkaProducer<String, String> producer;

    private static KafkaProducer<String, String> createProducer(){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.RETRIES_CONFIG, "3");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 200);
        props.put(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG, true);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        return producer;
    }
    public static void main( String[] args ) throws InterruptedException {
        RandomDataGenerator AdvertiseGenerator = new RandomDataGenerator();
        AdvertiseGenerator.reSeed(10000000L);

        if(null==producer){
            producer = createProducer();
        }
        // for loop to generate advertisement
        for (int i = 0; i < SENTENCE_NUM; ++i) {
            double t = AdvertiseGenerator.nextExponential(LAMBDA);
            String advId = UUID.randomUUID().toString();

            // TODO: write to kafka topic
            // write (t, advId) to kafka or wait t then write advId to kafka?
            Thread.sleep((long)(t*60*60));
            //producer.send(new ProducerRecord<String, String>(ADV_TOPIC, advId));
            System.out.println(String.valueOf(t) + "\t" + advId);

            // whether customer clicked this advertisement
            new ClickThread(advId).start();
        }

    }

    static class ClickThread implements Runnable{
        private double CLICK_PROBABILITY = 0.33;
        private String advId;
        private Thread t;

        public ClickThread(String advId){
            if (t == null) {
                t = new Thread (this);
            }
            this.advId = advId;
        }
        @Override
        public void run() {
            System.out.println("Start click thread");
            RandomDataGenerator clickGenerator = new RandomDataGenerator();

            if(clickGenerator.nextUniform(0,1)<=CLICK_PROBABILITY){
                double deltaT = clickGenerator.nextExponential(CLICK_LAMBDA);
                // TODO: write to kafka topic
                try {
                    Thread.sleep((long)(deltaT*60*60));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                //producer.send(new ProducerRecord<String, String>(ADV_TOPIC, advId));
                System.out.println("Clicked: " + String.valueOf(deltaT) + "\t" + advId);
            }
        }

        public void start () {
            t.start ();
        }
    }
}
