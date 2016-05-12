package fi.aalto.dmg;

import org.apache.storm.shade.com.google.common.cache.Cache;
import org.apache.storm.shade.com.google.common.cache.CacheLoader;
import org.apache.storm.shade.com.google.common.cache.LoadingCache;
import scala.Tuple2;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Created by jun on 30/01/16.
 */
public class CacheBuilder {
    public static void main(String[] args) throws InterruptedException {

        Cache<Integer, Integer> streamBuffer = org.apache.storm.shade.com.google.common.cache.CacheBuilder.newBuilder()
                .expireAfterWrite(100, TimeUnit.SECONDS)
                .weakKeys()
                .build();
        System.out.println(System.currentTimeMillis());
        for (int i = 0; i < 2000000; i++) {
            streamBuffer.put(i, i);
        }
        System.out.println(System.currentTimeMillis());

        for (int i = 5000; i < 50000; i++) {
            streamBuffer.getIfPresent(i);
        }
        System.out.println(System.currentTimeMillis());


    }
}
