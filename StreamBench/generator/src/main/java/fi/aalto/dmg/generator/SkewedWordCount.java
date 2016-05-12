package fi.aalto.dmg.generator;

import fi.aalto.dmg.statistics.ThroughputLog;
import fi.aalto.dmg.util.Constant;
import fi.aalto.dmg.util.FastZipfGenerator;
import fi.aalto.dmg.util.Utils;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

/**
 * Generator for WordCount workload
 * The distribution of works is skewed
 * Created by yangjun.wang on 26/10/15.
 */
public class SkewedWordCount extends Generator {
    private static final Logger logger = Logger.getLogger(SkewedWordCount.class);
    private static KafkaProducer<String, String> producer;

    private String TOPIC;
    private long SENTENCE_NUM = 1000000000;

    private int zipfSize;
    private double zipfExponent;
    private double mu;
    private double sigma;

    public SkewedWordCount() {
        super();
        producer = createBigBufferProducer();

        TOPIC = this.properties.getProperty("topic", "WordCount");
        zipfSize = Integer.parseInt(this.properties.getProperty("zipf.size", "10000"));
        zipfExponent = Double.parseDouble(this.properties.getProperty("zipf.exponent", "1"));
        mu = Double.parseDouble(this.properties.getProperty("sentence.mu", "10"));
        sigma = Double.parseDouble(this.properties.getProperty("sentence.sigma", "1"));
    }

    public void generate(int sleep_frequency) throws InterruptedException {

        RandomDataGenerator messageGenerator = new RandomDataGenerator();
        long time = System.currentTimeMillis();

        FastZipfGenerator zipfGenerator = new FastZipfGenerator(zipfSize, zipfExponent);
        ThroughputLog throughput = new ThroughputLog(this.getClass().getSimpleName());
        // for loop to generate message
        for (long sent_sentences = 0; sent_sentences < SENTENCE_NUM; ++sent_sentences) {
            double sentence_length = messageGenerator.nextGaussian(mu, sigma);
            StringBuilder messageBuilder = new StringBuilder();
            for (int l = 0; l < sentence_length; ++l) {
                int number = zipfGenerator.next();
                messageBuilder.append(Utils.intToString(number)).append(" ");
            }

            // Add timestamp
            messageBuilder.append(Constant.TimeSeparator).append(System.currentTimeMillis());

            throughput.execute();
            ProducerRecord<String, String> newRecord = new ProducerRecord<>(TOPIC, messageBuilder.toString());
            producer.send(newRecord);

            // control data generate speed
            if (sleep_frequency > 0 && sent_sentences % sleep_frequency == 0) {
                Thread.sleep(1);
            }
        }
        producer.close();
        logger.info("LatencyLog: " + String.valueOf(System.currentTimeMillis() - time));
    }

    public static void main(String[] args) throws InterruptedException {
        int SLEEP_FREQUENCY = -1;
        if (args.length > 0) {
            SLEEP_FREQUENCY = Integer.parseInt(args[0]);
        }
        new SkewedWordCount().generate(SLEEP_FREQUENCY);
    }
}

