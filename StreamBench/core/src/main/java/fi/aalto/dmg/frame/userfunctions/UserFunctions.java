package fi.aalto.dmg.frame.userfunctions;

import fi.aalto.dmg.frame.functions.MapFunction;

import java.util.Arrays;

/**
 * Created by yangjun.wang on 21/10/15.
 */
public class UserFunctions {

    public static Iterable<String> split(String str){
        // TODO: trim()
        return Arrays.asList(str.split("\\W+"));
    }

    public static <T extends Number> Double sum(T t1, T t2){
        return t1.doubleValue() + t2.doubleValue();
    }

    public static MapFunction<String, String> mapToSelf = new MapFunction<String, String>(){
        public String map(String var1) {
            return var1;
        }
    };

}
