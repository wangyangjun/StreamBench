package fi.aalto.dmg.generator;

import fi.aalto.dmg.statistics.Throughput;
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
    private static int SENTENCE_NUM = 100000;
    private static int ZIPF_SIZE = 100000;
    private static double ZIPF_EXPONENT = 1;
    private static String TOPIC = "WordCount";
    private static KafkaProducer<String, String> producer;

    private static KafkaProducer<String, String> createProducer(){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "zoo1:9092");
        props.put(ProducerConfig.RETRIES_CONFIG, "3");
        props.put(ProducerConfig.ACKS_CONFIG, "0"); // "all"
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1000);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 335544320);
        props.put(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG, true);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        return producer;
    }

    public static void main( String[] args ) {

        RandomDataGenerator messageGenerator = new RandomDataGenerator();
        long time = System.currentTimeMillis();

        if(null==producer){
            producer = createProducer();
        }

        RandomDataGenerator wordGenerator = new RandomDataGenerator();
        Throughput throughput = new Throughput(logger);
        // for loop to generate message
        for (int i = 0; i < SENTENCE_NUM; ++i) {
            // message size
            double sentence_length = messageGenerator.nextGaussian(10, 1);
            // generate word
            StringBuilder messageBuilder = new StringBuilder();
            for(int l = 0; l < 10; ++l){
                // get word length
//                int number = wordGenerator.nextZipf(ZIPF_SIZE, ZIPF_EXPONENT);
                int number = wordGenerator.nextInt(1, 5000);
                messageBuilder.append(Utils.intToString(number)).append(" ");
            }
            throughput.execute();
            ProducerRecord<String, String> newRecord = new ProducerRecord<String, String>(TOPIC, messageBuilder.toString());
            producer.send(newRecord);

//            System.out.println(messageBuilder.toString());
        }
//        System.out.println("Latency: " + String.valueOf(System.currentTimeMillis()-time));
    }
}

