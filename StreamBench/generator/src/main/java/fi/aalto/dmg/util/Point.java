package fi.aalto.dmg.util;

import java.io.Serializable;

/**
 * Created by jun on 09/03/16.
 */
public class Point implements Serializable {

    private long time;
    public int id; // Centroid id: 0, 1, 2, ...
    public double[] location;

    public Point() {
    }

    public Point(int id, double[] l) {
        this.id = id;
        this.location = l;
        this.time = System.currentTimeMillis();
    }

    public Point(int id, double[] l, long time) {
        this(id, l);
        this.time = time;
    }

    public Point(double[] l) {
        this(-1, l);
    }

    public Point(double[] l, long time) {
        this(-1, l);
        this.time = time;
    }

    public int dimension() {
        return this.location.length;
    }

    public boolean isCentroid() {
        return this.id >= 0;
    }

    public long getTime() {
        return this.time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public Point add(Point other) throws Exception {
        if (this.location.length != other.location.length) {
            throw new Exception("Dimensions of points are not equal");
        }
        double[] location = new double[this.location.length];
        for (int i = 0; i < this.location.length; ++i) {
            location[i] = this.location[i] + other.location[i];
        }
        return new Point(this.id, location, this.time);
    }

    public Point mul(long val) {
        double[] location = new double[this.location.length];
        for (int i = 0; i < this.location.length; ++i) {
            location[i] = this.location[i] * val;
        }
        return new Point(this.id, location, this.time);
    }

    public Point div(long val) {
        double[] location = new double[this.location.length];
        for (int i = 0; i < this.location.length; ++i) {
            location[i] = this.location[i] / val;
        }
        return new Point(this.id, location, this.time);
    }

    public double euclideanDistance(Point other) {
        return Math.sqrt(distanceSquaredTo(other));
    }

    public double distanceSquaredTo(Point other) {
        double squareSum = 0;
        for (int i = 0; i < this.location.length; ++i) {
            squareSum += Math.pow(this.location[i] - other.location[i], 2);
        }
        return squareSum;
    }

//    public void clear() {
//        x = y = 0.0;
//    }

    @Override
    public String toString() {
        String str = "(";
        for (int i = 0; i < this.location.length - 1; ++i) {
            str += this.location[i] + ", ";
        }
        str += this.location[this.location.length - 1] + ")";
        if (-1 != this.id)
            return id + ":" + str;
        return str;
    }

    public String positonString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < this.location.length - 1; ++i) {
            sb.append(this.location[i]).append(", ");
        }
        sb.append(this.location[location.length - 1]);
        return sb.toString();
    }
}
