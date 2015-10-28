package fi.aalto.dmg.frame;

import java.io.Serializable;

/**
 * Created by yangjun.wang on 24/10/15.
 */
abstract public class OperatorBase implements Serializable {
    protected int parallelism = -1;

    public int getParallelism(){
        return this.parallelism;
    }

    public void setParallelism(int parallelism){
        this.parallelism = parallelism;
    }

}
