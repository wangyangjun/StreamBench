package fi.aalto.dmg;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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

        for (String str : set) {
            System.out.println(str);
        }

        System.out.println(String.format("%05d", 123));

        int i = 0;
        while (i < 10) {
            System.out.println(++i);
        }
    }
}