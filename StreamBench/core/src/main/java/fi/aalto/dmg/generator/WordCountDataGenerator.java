package fi.aalto.dmg.generator;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.random.*;
import org.apache.log4j.Logger;

/**
 * Created by yangjun.wang on 26/10/15.
 */
public class WordCountDataGenerator {
    private static final Logger logger = Logger.getLogger(WordCountDataGenerator.class);

    public static void main( String[] args ) {
        RandomDataGenerator randomgenerator = new RandomDataGenerator();
        randomgenerator.reSeed(100);
        for (int i = 0; i < 100; i++) {
            String l = randomgenerator.nextHexString(20);
            logger.info(l);
        }

        RandomGenerator randomGenerator = new JDKRandomGenerator();
        randomGenerator.setSeed(17399225432l);  // Fixed seed means same results every time
        UniformRandomGenerator uniformRandomGenerator = new UniformRandomGenerator(randomGenerator);
        GaussianRandomGenerator rawGenerator = new GaussianRandomGenerator(randomGenerator);
        NormalDistribution normalizedRandomGenerator = new NormalDistribution(randomGenerator, 1, 1);


    }
}

