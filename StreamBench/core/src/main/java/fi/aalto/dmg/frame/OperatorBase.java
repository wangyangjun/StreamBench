package fi.aalto.dmg.frame;

import fi.aalto.dmg.exceptions.UnsupportOperatorException;

import java.io.Serializable;

/**
 * Created by yangjun.wang on 24/10/15.
 */
abstract public class OperatorBase implements Serializable {
    protected int parallelism = -1;
    protected boolean iterative_enabled = false;
    protected boolean isIterative_closed = false;

    public OperatorBase(int parallelism) {
        this.setParallelism(parallelism);
    }

    public int getParallelism() {
        return this.parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public void iterative() {
        this.iterative_enabled = true;
    }

    abstract public void closeWith(OperatorBase stream, boolean broadcast) throws UnsupportOperatorException;

    abstract public void print();

}
