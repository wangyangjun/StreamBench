package fi.aalto.dmg.util;

import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Exchanger;

/**
 * Binary tree for window data structure
 * fixed size tree, doesn't need support add node, remove node
 * Created by jun on 16/11/15.
 */
public class BTree<T> implements Serializable {
    public class Children{
        private int child1;
        private int child2;

        public Children(int c1, int c2) {
            this.child1 = c1;
            this.child2 = c2;
        }

        public boolean contains(int index){
            if(child1==index || child2==index) return true;
            return false;
        }

        public String toString(){
            return String.format("Child1:%d, Child2:%d", child1, child2);
        }

        public int getChild1(){ return child1; }
        public int getChild2(){ return child2; }
    }

    private List<T> dataContainer;
    private int size; // the number of leaves

    public BTree(int size) {
        // initialCapacity should be calculated with size
        // first size elements are leaves node
        this.size = size;
        dataContainer = new ArrayList<>(2*size-1);
        for(int i=0; i<2*size-1; ++i){
            dataContainer.add(i, null);
        }
    }

    public T get(int index){
        return dataContainer.get(index);
    }

    public void set(int index, T value){
        dataContainer.set(index, value);
    }

    public int getSize(){ return this.size; }

    /**
     * @param nodeIndex the index of a node in dataContainer
     * @return index of its parent node
     */
    public int findParent(int nodeIndex){
        return findParent(nodeIndex, size);
    }

    private int findParent(int nodeIndex, int leavesSize){
        // if root node
        if(isRoot(nodeIndex, leavesSize)) return nodeIndex;
        // if last leave node is odd node(0,1,2)
        if(leavesSize%2==1 && nodeIndex==leavesSize-1){
            // find parent of this odd node in a other tmp tree
            // (remove all leave nodes except last odd node)
            int tmpLeavesSize = (leavesSize+1)/2;
            int tmpParent = findParent(tmpLeavesSize-1, tmpLeavesSize);
//            System.out.println(String.format("Tmp tree: size=%d, index=%d, parent=%d ", tmpLeavesSize, tmpLeavesSize-1, tmpParent));
            return tmpParent + leavesSize - 1;
        }
        // if leave node except last odd leave node
        if(nodeIndex<leavesSize)
            return leavesSize + nodeIndex/2;
        // if not leave node
        else{
            // find parent of this node in a tmp tree
            // remove all leave nodes except last odd node
            if(leavesSize%2==0){ // remove all leaves
                int tmpLeavesSize = leavesSize/2;
                int tmpNodeIndex = nodeIndex-leavesSize;
                int tmpParent = findParent(tmpNodeIndex, tmpLeavesSize);
//                System.out.println(String.format("Tmp tree: size=%d, index=%d, parent=%d ", tmpLeavesSize, tmpNodeIndex, tmpParent));
                return tmpParent + leavesSize;
            } else { // remove leavesSize-1 leaves
                // the last odd leave in old tree will be last leave in the new tree
                int tmpLeavesSize = (leavesSize+1)/2;
                int tmpNodeIndex = nodeIndex-leavesSize;
                if( (nodeIndex+1)>=leavesSize+tmpLeavesSize ){ ++tmpNodeIndex; }

                int tmpParent = findParent(tmpNodeIndex, tmpLeavesSize);
//                System.out.println(String.format("Tmp tree: size=%d, index=%d, parent=%d ", tmpLeavesSize, tmpNodeIndex, tmpParent));
                return tmpParent + leavesSize - 1;
            }
        }
    }

    public Children findChildren(int nodeIndex){
        return findChildren(nodeIndex, size);
    }

    private Children findChildren(int nodeIndex, int leavesSize){
        // if leave node, no child
        if(nodeIndex < leavesSize){
            return new Children(-1, -1);
        }
        // if it is on the second floor
        if(nodeIndex < (leavesSize+leavesSize/2)){
            // index in the new tree (remove one floor)
            int tmpIndex = nodeIndex-leavesSize;
            // get its children
            return new Children(tmpIndex*2, tmpIndex*2+1);
        } else {
            // remove one floor
            if(leavesSize%2==0){ // remove all leaves
                int tmpLeavesSize = leavesSize/2;
                int tmpNodeIndex = nodeIndex-leavesSize;
                Children tmpChildren = findChildren(tmpNodeIndex, tmpLeavesSize);
                Children children = new Children(tmpChildren.child1+leavesSize, tmpChildren.child2+leavesSize);
//                System.out.println(String.format("Tmp tree: size=%d, index=%d", tmpLeavesSize, tmpNodeIndex));
//                System.out.println("Children in tmp tree:" + tmpChildren);
//                System.out.println("Children in tree:" + children);
                return children;
            } else { // remove leavesSize-1 leaves
                // the last odd leave in old tree will be last leave in the new tree
                int tmpLeavesSize = (leavesSize+1)/2;
                // nodeIndex couldn't on the second floor here
                int tmpNodeIndex = nodeIndex-leavesSize+1;
                Children tmpChildren = findChildren(tmpNodeIndex, tmpLeavesSize);
//                System.out.println(String.format("Tmp tree: size=%d, index=%d", tmpLeavesSize, tmpNodeIndex));
//                System.out.println("Children in tmp tree:" + tmpChildren);

                // whether last odd node in children
                if(tmpChildren.child1 > tmpLeavesSize-1){
                    tmpChildren.child1 += leavesSize-1;
                } else if(tmpChildren.child1 < tmpLeavesSize-1){
                    tmpChildren.child1 += leavesSize;
                } else {
                    tmpChildren.child1 = leavesSize-1;
                }

                if(tmpChildren.child2 > tmpLeavesSize-1){
                    tmpChildren.child2 += leavesSize-1;
                } else if(tmpChildren.child2 < tmpLeavesSize-1){
                    tmpChildren.child2 += leavesSize;
                } else {
                    tmpChildren.child2 = leavesSize-1;
                }
//                System.out.println("Children in tree:" + tmpChildren);

                return tmpChildren;
            }
        }
    }

    public boolean isRoot(int nodeIndex){
        return isRoot(nodeIndex, size);
    }

    private boolean isRoot(int nodeIndex, int leavesSize) {
        return nodeIndex == 2 * leavesSize - 2;
    }

    public T getRoot() {
        return dataContainer.get(2 * size - 2);
    }
}
