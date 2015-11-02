package fi.aalto.dmg.generator;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.commons.math3.random.*;
import org.apache.log4j.Logger;

/**
 * Created by yangjun.wang on 26/10/15.
 */
public class WordCountDataGenerator {
    private static final Logger logger = Logger.getLogger(WordCountDataGenerator.class);
    private static Integer SETANCE_NUM = 100;
    private static Integer ZIPF_SIZE = 100000;
    private static long ZIPF_EXPONENT = 1L;

    public static void main( String[] args ) {
        RandomDataGenerator messageGenerator = new RandomDataGenerator();
        messageGenerator.reSeed(1000000);

        RandomDataGenerator wordGenerator = new RandomDataGenerator();
        wordGenerator.reSeed(10000000000L);
        // for loop to generate message
        for (int i = 0; i < SETANCE_NUM; ++i) {
            // message size
            Double sentence_length_double = messageGenerator.nextGaussian(10, 1);
            Integer sentence_length = sentence_length_double.intValue();
            // generate word
            StringBuilder messageBuilder = new StringBuilder();
            for(int l = 0; l < sentence_length; ++l){
                // get word length
                Integer zipf = wordGenerator.nextZipf(ZIPF_SIZE, ZIPF_EXPONENT);
                messageBuilder.append(Integer.toHexString(zipf).toUpperCase()).append(" ");
            }
            System.out.println(messageBuilder.toString());
        }

    }
}

