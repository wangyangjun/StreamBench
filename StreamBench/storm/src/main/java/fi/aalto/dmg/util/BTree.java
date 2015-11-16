package fi.aalto.dmg.util;

import java.util.ArrayList;
import java.util.List;

/**
 * Binary tree for window data structure
 * fixed size tree, doesn't need support add node, remove node
 * Created by jun on 16/11/15.
 */
public class BTree<T> {
    private List<T> dataContainer;
    private int size; // the number of leaves

    public BTree(int size) {
        // initialCapacity should be calculated with size
        // first size elements are leaves node
        this.size = size;
        dataContainer = new ArrayList<>(2*size-1);
    }

    /**
     * @param nodeIndex the index of a node in dataContainer
     * @return index of its parent node
     */
    public int findParent(int nodeIndex){
        return findParent(nodeIndex, size);
    }

    public int findParent(int nodeIndex, int leavesSize){
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
            if(leavesSize%2==0){
                int tmpLeavesSize = leavesSize/2;
                int tmpParent = findParent(nodeIndex-leavesSize, tmpLeavesSize);
//                System.out.println(String.format("Tmp tree: size=%d, index=%d, parent=%d ", tmpLeavesSize, nodeIndex-leavesSize, tmpParent));
                return tmpParent + leavesSize;
            } else {
                // the last odd leave in old tree will be last leave in the new tree
                int tmpLeavesSize = (leavesSize+1)/2;
                int tmpNodeIndex = nodeIndex-leavesSize;
                // if node is still not leave in the new tree
                if( (nodeIndex+1)>=leavesSize+tmpLeavesSize ){ ++tmpNodeIndex; }
                int tmpParent = findParent(tmpNodeIndex, tmpLeavesSize);
//                System.out.println(String.format("Tmp tree: size=%d, index=%d, parent=%d ", tmpLeavesSize, tmpNodeIndex, tmpParent));
                return tmpParent + leavesSize - 1;
            }
        }
    }


    public boolean isRoot(int nodeIndex){
        return isRoot(nodeIndex, size);
    }

    private boolean isRoot(int nodeIndex, int leavesSize) {
        return nodeIndex == 2 * leavesSize - 2;
    }
}
