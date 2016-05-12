package fi.aalto.dmg;


import fi.aalto.dmg.util.Constant;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by jun on 24/01/16.
 */
public class RecordTimeSeparatorTest {

    @Test
    public void SeparatorTest() {
        String str = "k23j4 2k34j 2kl34 23jk4 ";
        String[] list = str.split(Constant.TimeSeparatorRegex);
        System.out.println(list[0]);
        System.out.println(list.length);
        Assert.assertTrue(true);
    }
}
