package fi.aalto.dmg.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by jun on 16/11/15.
 */
public class BTreeTest {

    @Test
    public void findParent(){
        BTree<Integer> tree = new BTree<>(13);
        Assert.assertEquals(tree.findParent(0), 13);
        Assert.assertEquals(tree.findParent(12), 23);
        Assert.assertEquals(tree.findParent(13), 19);
        Assert.assertEquals(tree.findParent(16), 20);
        Assert.assertEquals(tree.findParent(17), 21);
        Assert.assertEquals(tree.findParent(19), 22);
        Assert.assertEquals(tree.findParent(22), 24);
        Assert.assertEquals(tree.findParent(23), 24);
        Assert.assertEquals(tree.findParent(24), 24);


        BTree<Integer> tree2 = new BTree<>(12);
        Assert.assertEquals(tree2.findParent(0), 12);
        Assert.assertEquals(tree2.findParent(7), 15);
        Assert.assertEquals(tree2.findParent(8), 16);
        Assert.assertEquals(tree2.findParent(11), 17);
        Assert.assertEquals(tree2.findParent(12), 18);
        Assert.assertEquals(tree2.findParent(17), 20);
        Assert.assertEquals(tree2.findParent(19), 21);
        Assert.assertEquals(tree2.findParent(20), 22);
//        Assert.assertEquals(tree2.findParent(22), 22);


    }
}
