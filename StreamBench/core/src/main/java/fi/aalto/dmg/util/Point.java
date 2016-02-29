package fi.aalto.dmg.util;

import java.io.Serializable;

/**
 * Created by jun on 26/02/16.
 */

// *************************************************************************
//     DATA TYPES
// *************************************************************************

/**
 * A simple two-dimensional point.
 */
public class Point implements Serializable {

    private long time;
    public int id; // Centroid id: 0, 1, 2, ...
    public double x, y;

    public Point() {}

    public Point(int id, double x, double y) {
        this.x = x;
        this.y = y;
        this.id = id;
        this.time = System.currentTimeMillis();
    }

    public Point(int id, double x, double y, long time) {
        this(id, x, y);
        this.time = time;
    }

    public Point(double x, double y) {
        this(-1, x, y);
    }

    public Point(double x, double y, long time) {
        this(-1, x, y);
        this.time = time;
    }


    public boolean isCentroid() {
        return this.id >= 0;
    }

    public long getTime() { return this.time; }

    public void setTime(long time) { this.time = time; }

    public Point add(Point other) {
        x += other.x;
        y += other.y;
        return this;
    }

    public Point mul(long val) {
        x *= val;
        y *= val;
        return this;
    }

    public Point div(long val) {
        x /= val;
        y /= val;
        return this;
    }

    public double euclideanDistance(Point other) {
        return Math.sqrt((x-other.x)*(x-other.x) + (y-other.y)*(y-other.y));
    }

    public void clear() {
        x = y = 0.0;
    }

    @Override
    public String toString() {
        if(-1 != this.id)
            return id + ":(" + x + ", " + y + ")";
        return "(" + x + ", " + y + ")";
    }
}
