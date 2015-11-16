package fi.aalto.dmg;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by jun on 11/9/15.
 */
public class UtilsTest {

    @Test
    public void longDivideTest()
    {
        long a = 100500L;
        long b = a/1000L;
        System.out.print(b);
        Assert.assertTrue(true);
    }

    @Test
    public void mapListTest(){
        List<Map<String, Integer>> maps = new ArrayList<>(3);

    }

    @Test
    public void doWhileTest(){
        int i = 0;
        int j = 0;
        while (i<5){
            System.out.println(i);
            ++i;
        }
        do{
            System.out.println(j);
            ++j;
        } while(j<5);
    }
}
