package fi.aalto.dmg;

import org.apache.commons.math3.distribution.MultivariateNormalDistribution;

/**
 * Created by jun on 09/03/16.
 */
public class GaussianPoints {
    public static void main(String[] args) {
        double[] means = new double[]{10, 10};
        double[][] covariances = new double[][]{{3, 2}, {2, 3}};

        for (int i = 0; i < 300; i++) {
            MultivariateNormalDistribution distribution = new MultivariateNormalDistribution(means, covariances);
            System.out.println(String.valueOf(distribution.sample()[0]) + ", " + String.valueOf(distribution.sample()[1]));
        }
    }
}
