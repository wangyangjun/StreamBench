package fi.aalto.dmg;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by jun on 27/11/15.
 */
public class SetTest {
    public static void main(String[] args) throws Exception {
        Set<String> set = new HashSet<>();
        set.add("Hello");
        set.add("World");
        set.add("Hello");

        for(String str : set){
            System.out.println(str);
        }
    }
}