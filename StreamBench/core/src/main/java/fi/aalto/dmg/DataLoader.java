package fi.aalto.dmg;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.*;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Created by yangjun.wang on 27/10/15.
 */
public class DataLoader {
    public static void main( String[] args ) throws ExecutionException, InterruptedException, IOException {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String,String> producer = new KafkaProducer<String,String>(props);
        String topic="WordCount";
        String key = "key";

        InputStream stream = DataLoader.class.getClassLoader().getResourceAsStream("wikipedia.txt");
        BufferedReader in = new BufferedReader(new InputStreamReader(stream));
        String line = null;

        StringBuilder responsweData = new StringBuilder();
        while((line = in.readLine()) != null) {
            ProducerRecord<String,String> producerRecord = new ProducerRecord<String,String>(topic, key, line);
            // producer.send(producerRecord).get(); // sync
            producer.send(producerRecord);
        }

        producer.close();
    }
}
