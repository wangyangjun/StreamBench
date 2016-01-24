package fi.aalto.dmg.generator;

import fi.aalto.dmg.statistics.Throughput;
import fi.aalto.dmg.util.Constant;
import fi.aalto.dmg.util.FastZipfGenerator;
import fi.aalto.dmg.util.Utils;
import org.apache.commons.math3.random.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.util.Properties;

/**
 * Generator for WordCount workload
 * Created by yangjun.wang on 26/10/15.
 */
public class WordCountDataGenerator {
    private static final Logger logger = Logger.getLogger(WordCountDataGenerator.class);
    private static long SENTENCE_NUM = 30000000;
    private static int ZIPF_SIZE = 10000;
    private static double ZIPF_EXPONENT = 1;
    private static String TOPIC = "WordCount";
    private static KafkaProducer<String, String> producer;


    public static void main( String[] args ) throws InterruptedException {
        // 1   ---- 0.7K/s
        // 2   ---- 1.3K/s
        // 3   ---- 2.0K/s
        // 4   ---- 2.6K/s
        // 5   ---- 3.2K/s
        // 10  ---- 6K/s
        // 50  ---- 15K/s
        // 100 ---- 27K/s
        int SLEEP_FREQUENCY = 5;
        if(args.length > 0) {
            SLEEP_FREQUENCY = Integer.parseInt(args[0]);
        }

        RandomDataGenerator messageGenerator = new RandomDataGenerator();
        long time = System.currentTimeMillis();

        if(null==producer){
            producer = Generator.createProducer();
        }

        FastZipfGenerator zipfGenerator = new FastZipfGenerator(ZIPF_SIZE, ZIPF_EXPONENT);
        Throughput throughput = new Throughput("WordCountDataGenerator");
        // for loop to generate message
        for (long sent_sentences = 0; sent_sentences < SENTENCE_NUM; ++sent_sentences) {
            double sentence_length = messageGenerator.nextGaussian(10, 1);
            StringBuilder messageBuilder = new StringBuilder();
            for(int l = 0; l < sentence_length; ++l){
                // TODO: switch between Uniform words or skewed words
//                int number = messageGenerator.nextInt(1, 5000);
                int number = zipfGenerator.next();
                messageBuilder.append(Utils.intToString(number)).append(" ");
            }

            // Add timestamp
            messageBuilder.append(Constant.TimeSeparator).append(System.currentTimeMillis());
            throughput.execute();
            ProducerRecord<String, String> newRecord = new ProducerRecord<String, String>(TOPIC, messageBuilder.toString());
            producer.send(newRecord);

            // control data generate speed
            if(sent_sentences%SLEEP_FREQUENCY == 0) {
                Thread.sleep(1);
            }
        }
        logger.info("Latency: " + String.valueOf(System.currentTimeMillis()-time));
    }
}

