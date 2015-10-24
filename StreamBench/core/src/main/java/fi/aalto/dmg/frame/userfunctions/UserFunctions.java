package fi.aalto.dmg.frame.userfunctions;

import java.util.Arrays;

/**
 * Created by yangjun.wang on 21/10/15.
 */
public class UserFunctions {

    public static Iterable<String> split(String str){
        // TODO: trim()
        return Arrays.asList(str.split("\\W+"));
    }
}
