package fi.aalto.dmg;

import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedList;
import java.util.ListIterator;

/**
 * Created by jun on 18/11/15.
 */
public class LinkedListTest {
    @Test
    public void forLoopTest()
    {
        LinkedList<Integer> linkedList = new LinkedList<>();
        for(int i=0; i<10; ++i){
            linkedList.add(i);
        }
        ListIterator<Integer> listIterator = linkedList.listIterator();
        for(Integer integer : linkedList){
            System.out.println(integer);
        }
        Assert.assertTrue(true);
    }
}
