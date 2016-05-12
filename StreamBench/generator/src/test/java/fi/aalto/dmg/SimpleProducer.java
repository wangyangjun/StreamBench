package fi.aalto.dmg;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by jun on 29/02/16.
 */
public class SimpleProducer {
    public static void main(String[] args) throws InterruptedException {
        int SLEEP_FREQUENCY = 50;
        if (args.length > 0) {
            SLEEP_FREQUENCY = Integer.parseInt(args[0]);
        }

        long time = System.currentTimeMillis();

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.RETRIES_CONFIG, "3");
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "100");
        props.put(ProducerConfig.ACKS_CONFIG, "1"); // "all"
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        StringBuilder messageBuilder = new StringBuilder();

        // Add timestamp
        messageBuilder.append("23.23")
                .append("\t")
                .append("-34.23")
                .append("\t")
                .append(String.valueOf(time));

        ProducerRecord<String, String> newRecord = new ProducerRecord<String, String>("KMeans", messageBuilder.toString());
        producer.send(newRecord);
        producer.close();

    }
}
