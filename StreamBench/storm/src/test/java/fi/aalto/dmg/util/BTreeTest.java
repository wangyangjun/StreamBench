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
    public void findParent() {
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
        Assert.assertEquals(tree2.findParent(16), 20);
        Assert.assertEquals(tree2.findParent(17), 20);
        Assert.assertEquals(tree2.findParent(19), 21);
        Assert.assertEquals(tree2.findParent(20), 22);

        BTree<Integer> tree3 = new BTree<>(11);
        System.out.println(tree3.findParent(10));
        System.out.println(tree3.findParent(14));
    }

    @Test
    public void findChildren() {
        BTree<Integer> tree = new BTree<>(13);
        Assert.assertTrue(tree.findChildren(13).contains(0));
        Assert.assertTrue(tree.findChildren(13).contains(1));
        Assert.assertTrue(tree.findChildren(17).contains(8));
        Assert.assertTrue(tree.findChildren(17).contains(9));
        Assert.assertTrue(tree.findChildren(19).contains(13));
        Assert.assertTrue(tree.findChildren(19).contains(14));
        Assert.assertTrue(tree.findChildren(21).contains(17));
        Assert.assertTrue(tree.findChildren(21).contains(18));
        Assert.assertTrue(tree.findChildren(22).contains(19));
        Assert.assertTrue(tree.findChildren(22).contains(20));
        Assert.assertTrue(tree.findChildren(23).contains(21));
        Assert.assertTrue(tree.findChildren(23).contains(12));
        Assert.assertTrue(tree.findChildren(24).contains(22));
        Assert.assertTrue(tree.findChildren(24).contains(23));

        BTree<Integer> tree2 = new BTree<>(12);
        Assert.assertTrue(tree2.findChildren(13).contains(2));
        Assert.assertTrue(tree2.findChildren(13).contains(3));
        Assert.assertTrue(tree2.findChildren(15).contains(6));
        Assert.assertTrue(tree2.findChildren(15).contains(7));
        Assert.assertTrue(tree2.findChildren(18).contains(12));
        Assert.assertTrue(tree2.findChildren(18).contains(13));
        Assert.assertTrue(tree2.findChildren(20).contains(16));
        Assert.assertTrue(tree2.findChildren(20).contains(17));
        Assert.assertTrue(tree2.findChildren(21).contains(18));
        Assert.assertTrue(tree2.findChildren(21).contains(19));
        Assert.assertTrue(tree2.findChildren(22).contains(21));
        Assert.assertTrue(tree2.findChildren(22).contains(20));

        BTree<Integer> tree3 = new BTree<>(10);
        Assert.assertTrue(tree3.findChildren(18).contains(14));
        Assert.assertTrue(tree3.findChildren(18).contains(17));

    }
}
