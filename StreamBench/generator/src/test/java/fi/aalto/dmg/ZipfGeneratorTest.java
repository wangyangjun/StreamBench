package fi.aalto.dmg;

/**
 * Created by jun on 10/12/15.
 */

import fi.aalto.dmg.util.FastZipfGenerator;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

public class ZipfGeneratorTest {
    public static void main(String[] args) {

        int size = 10;
        double skew = 2.0;

        ZipfGenerator z0 = new ZipfGenerator(size, skew);
        FastZipfGenerator z1 = new FastZipfGenerator(size, skew);

        long before = 0;
        long after = 0;

        int n = 5000000;

        before = System.nanoTime();
        Map<Integer, Integer> counts0 = computeCounts(z0, size, n);
        after = System.nanoTime();
        System.out.println(counts0 + ", duration " + (after - before) / 1e6);

        before = System.nanoTime();
        Map<Integer, Integer> counts1 = computeCounts(z1, size, n);
        after = System.nanoTime();
        System.out.println(counts1 + ", duration " + (after - before) / 1e6);
    }

    private static Map<Integer, Integer> computeCounts(
            ZipfGenerator z, int size, int n) {
        Map<Integer, Integer> counts = new LinkedHashMap<Integer, Integer>();
        for (int i = 1; i <= size; i++) {
            counts.put(i, 0);
        }
        for (int i = 1; i <= n; i++) {
            int k = z.next();
            counts.put(k, counts.get(k) + 1);
        }
        return counts;
    }

    private static Map<Integer, Integer> computeCounts(
            FastZipfGenerator z, int size, int n) {
        Map<Integer, Integer> counts = new LinkedHashMap<Integer, Integer>();
        for (int i = 1; i <= size; i++) {
            counts.put(i, 0);
        }
        for (int i = 1; i <= n; i++) {
            int k = z.next();
            counts.put(k, counts.get(k) + 1);
        }
        return counts;
    }

}

// Based on http://diveintodata.org/tag/zipf/
class ZipfGenerator {
    private Random rnd = new Random(0);
    private int size;
    private double skew;
    private double bottom = 0;

    public ZipfGenerator(int size, double skew) {
        this.size = size;
        this.skew = skew;

        for (int i = 1; i <= size; i++) {
            this.bottom += (1 / Math.pow(i, this.skew));
        }
    }

    // the next() method returns an random rank id.
    // The frequency of returned rank ids are follows Zipf distribution.
    public int next() {
        int rank;
        double friquency = 0;
        double dice;

        rank = rnd.nextInt(size) + 1;
        friquency = (1.0d / Math.pow(rank, this.skew)) / this.bottom;
        dice = rnd.nextDouble();

        while (!(dice < friquency)) {
            rank = rnd.nextInt(size) + 1;
            friquency = (1.0d / Math.pow(rank, this.skew)) / this.bottom;
            dice = rnd.nextDouble();
        }

        return rank;
    }


    // This method returns a probability that the given rank occurs.
    public double getProbability(int rank) {
        return (1.0d / Math.pow(rank, this.skew)) / this.bottom;
    }
}
