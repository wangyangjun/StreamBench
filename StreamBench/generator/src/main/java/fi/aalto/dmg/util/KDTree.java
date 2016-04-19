//package fi.aalto.dmg.util;
//
//import java.util.ArrayDeque;
//import java.util.Queue;
//
///**
// * Created by jun on 09/03/16.
// */
//public class KDTree {
//    private int size = 0;
//
//    private enum Orientation {
//        LeftRight, AboveBelow;
//
//        public Orientation next() {
//            if (this.equals(Orientation.AboveBelow))
//                return Orientation.LeftRight;
//
//            return Orientation.AboveBelow;
//        }
//    }
//
//    private static class Node {
//        /*
//         * the point
//         */
//        private Point p;
//        /*
//         * the axis-aligned rectangle corresponding to this/ node
//         */
//        private RectHV rect;
//        /*
//         * the left/bottom subtree
//         */
//        private Node lb;
//        /*
//         * the right/top subtree
//         */
//        private Node rt;
//
//        public Node(Point p) {
//            this.p = p;
//        }
//    }
//
//    private Node root;
//
//    /*
//     * construct an empty set of points
//     */
//    public KDTree() {
//    }
//
//    /*
//     * is the set empty?
//     */
//    public boolean isEmpty() {
//        return root == null;
//    }
//
//    /*
//     * number of points in the set
//     */
//    public int size() {
//        return size;
//    }
//
//    /*
//     * add the point p to the set (if it is not already in the set)
//     */
//    public void insert(Point p) {
//        if (isEmpty()) {
//            root = new Node(p);
//            root.rect = new RectHV(-50, -50, 50, 50);
//            size++;
//            return;
//        }
//        root = put(root, p, Orientation.LeftRight);
//    }
//
//    private Node put(Node x, Point p, Orientation orientation) {
//        if (x == null) {
//            size++;
//            return new Node(p);
//        }
//        if (x.p.equals(p)) {
//            return x;
//        }
//        int cmp = compare(p, x.p, orientation);
//        Orientation nextOrientation = orientation.next();
//        if (cmp < 0) {
//            x.lb = put(x.lb, p, nextOrientation);
//            if (x.lb.rect == null) {
//                if (orientation == Orientation.LeftRight) {
//                    x.lb.rect = new RectHV(x.rect.xmin(), x.rect.ymin(),
//                            x.p.x(), x.rect.ymax());
//                } else {
//                    x.lb.rect = new RectHV(x.rect.xmin(), x.rect.ymin(),
//                            x.rect.xmax(), x.p.y());
//                }
//            }
//        } else {
//            x.rt = put(x.rt, p, nextOrientation);
//            if (x.rt.rect == null) {
//                if (orientation == Orientation.LeftRight) {
//                    x.rt.rect = new RectHV(x.p.x(), x.rect.ymin(),
//                            x.rect.xmax(), x.rect.ymax());
//                } else {
//                    x.rt.rect = new RectHV(x.rect.xmin(), x.p.y(),
//                            x.rect.xmax(), x.rect.ymax());
//                }
//            }
//        }
//        return x;
//    }
//
//    private int compare(Point p, Point q, Orientation orientation) {
//        if (orientation == Orientation.LeftRight) {
//            return Double.compare(p.x(), q.x());
//        } else {
//            return Double.compare(p.y(), q.y());
//        }
//    }
//
//    /*
//     * does the set contain the point p?
//     */
//    public boolean contains(Point p) {
//        return contains(root, p, Orientation.LeftRight);
//    }
//
//    private boolean contains(Node x, Point p, Orientation orientation) {
//        if (x == null) {
//            return false;
//        }
//        if (x.p.equals(p)) {
//            return true;
//        }
//        int cmp = compare(p, x.p, orientation);
//        Orientation nextOrientation = orientation.next();
//        if (cmp < 0) {
//            return contains(x.lb, p, nextOrientation);
//        } else {
//            return contains(x.rt, p, nextOrientation);
//        }
//    }
//
//    /*
//     * all points in the set that are inside the rectangle
//     */
//    public Iterable<Point> range(RectHV rect) {
//        Queue<Point> queue = new ArrayDeque<>();
//
//        if (!isEmpty()) {
//            findPoints(queue, rect, root);
//        }
//        return queue;
//    }
//
//    private void findPoints(Queue<Point> queue, RectHV rect, Node x) {
//        if (!rect.intersects(x.rect)) {
//            return;
//        }
//        if (rect.contains(x.p)) {
//            queue.add(x.p);
//        }
//        if (x.lb != null) {
//            findPoints(queue, rect, x.lb);
//        }
//        if (x.rt != null) {
//            findPoints(queue, rect, x.rt);
//        }
//    }
//
//    /*
//     * a nearest neighbor in the set to p; null if set is empty
//     */
//    public Point nearest(Point p) {
//        if (isEmpty()) {
//            return null;
//        }
//        return findNearest(root, p, root.p, Double.MAX_VALUE,
//                Orientation.LeftRight);
//    }
//
//    private Point findNearest(Node x, Point p, Point nearest,
//                                double nearestDistance, Orientation orientation) {
//        if (x == null) {
//            return nearest;
//        }
//        Point closest = nearest;
//        double closestDistance = nearestDistance;
//        double distance = x.p.distanceSquaredTo(p);
//        if (distance < nearestDistance) {
//            closest = x.p;
//            closestDistance = distance;
//        }
//        Node first, second;
//        if (orientation == Orientation.LeftRight) {
//            if (p.x() < x.p.x()) {
//                first = x.lb;
//                second = x.rt;
//            } else {
//                first = x.rt;
//                second = x.lb;
//            }
//        } else {
//            if (p.y() < x.p.y()) {
//                first = x.lb;
//                second = x.rt;
//            } else {
//                first = x.rt;
//                second = x.lb;
//            }
//        }
//        Orientation nextOrientation = orientation.next();
//        if (first != null && first.rect.distanceSquaredTo(p) < closestDistance) {
//            closest = findNearest(first, p, closest, closestDistance,
//                    nextOrientation);
//            closestDistance = closest.distanceSquaredTo(p);
//        }
//        if (second != null
//                && second.rect.distanceSquaredTo(p) < closestDistance) {
//            closest = findNearest(second, p, closest, closestDistance,
//                    nextOrientation);
//        }
//
//        return closest;
//    }
//}
