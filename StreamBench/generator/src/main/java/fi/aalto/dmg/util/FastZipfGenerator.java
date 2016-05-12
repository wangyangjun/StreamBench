package fi.aalto.dmg.util;

import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;

/**
 * Created by jun on 12/12/15.
 */
public class FastZipfGenerator {
    private Random random = new Random(0);
    private NavigableMap<Double, Integer> map;

    public FastZipfGenerator(int size, double skew) {
        map = computeMap(size, skew);
    }

    private static NavigableMap<Double, Integer> computeMap(
            int size, double skew) {
        NavigableMap<Double, Integer> map =
                new TreeMap<Double, Integer>();

        double div = 0;
        for (int i = 1; i <= size; i++) {
            div += (1 / Math.pow(i, skew));
        }

        double sum = 0;
        for (int i = 1; i <= size; i++) {
            double p = (1.0d / Math.pow(i, skew)) / div;
            sum += p;
            map.put(sum, i - 1);
        }
        return map;
    }

    public int next() {
        double value = random.nextDouble();
        return map.ceilingEntry(value).getValue() + 1;
    }

}