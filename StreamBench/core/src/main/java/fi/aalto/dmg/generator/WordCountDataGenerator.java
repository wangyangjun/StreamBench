package fi.aalto.dmg.generator;

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
    private static int SENTENCE_NUM = 300000;
    private static int ZIPF_SIZE = 5000;
    private static double ZIPF_EXPONENT = 1;
    private static String TOPIC = "WordCount";
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

    public static void main( String[] args ) {

        RandomDataGenerator messageGenerator = new RandomDataGenerator();
        long time = System.currentTimeMillis();

        if(null==producer){
            producer = createProducer();
        }

        RandomDataGenerator wordGenerator = new RandomDataGenerator();
        // for loop to generate message
        for (int i = 0; i < SENTENCE_NUM; ++i) {
            // message size
            double sentence_length = messageGenerator.nextGaussian(10, 1);
            // generate word
            StringBuilder messageBuilder = new StringBuilder();
            for(int l = 0; l < sentence_length; ++l){
                // get word length
//                int number = wordGenerator.nextZipf(ZIPF_SIZE, ZIPF_EXPONENT);
                int number = wordGenerator.nextInt(1, 5000);
                messageBuilder.append(Utils.intToString(number)).append(" ");
            }
            producer.send(new ProducerRecord<String, String>(TOPIC, messageBuilder.toString()));

//            System.out.println(messageBuilder.toString());
        }
//        System.out.println("Latency: " + String.valueOf(System.currentTimeMillis()-time));
    }
}

