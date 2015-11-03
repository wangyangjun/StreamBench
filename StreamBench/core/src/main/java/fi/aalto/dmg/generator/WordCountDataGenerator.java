package fi.aalto.dmg.generator;

import fi.aalto.dmg.util.Utils;
import org.apache.commons.math3.random.*;
import org.apache.log4j.Logger;

/**
 * Created by yangjun.wang on 26/10/15.
 */
public class WordCountDataGenerator {
    private static final Logger logger = Logger.getLogger(WordCountDataGenerator.class);
    private static int SENTENCE_NUM = 100;
    private static int ZIPF_SIZE = 100000;
    private static long ZIPF_EXPONENT = 1L;

    public static void main( String[] args ) {
        RandomDataGenerator messageGenerator = new RandomDataGenerator();
        messageGenerator.reSeed(1000000);

        RandomDataGenerator wordGenerator = new RandomDataGenerator();
        wordGenerator.reSeed(10000000000L);
        // for loop to generate message
        for (int i = 0; i < SENTENCE_NUM; ++i) {
            // message size
            double sentence_length = messageGenerator.nextGaussian(10, 1);
            // generate word
            StringBuilder messageBuilder = new StringBuilder();
            for(int l = 0; l < sentence_length; ++l){
                // get word length
                int zipf = wordGenerator.nextZipf(ZIPF_SIZE, ZIPF_EXPONENT);
                messageBuilder.append(Utils.intToString(zipf)).append(" ");
            }
            System.out.println(messageBuilder.toString());
        }

    }
}

