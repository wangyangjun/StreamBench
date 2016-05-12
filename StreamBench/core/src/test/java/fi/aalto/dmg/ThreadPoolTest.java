package fi.aalto.dmg;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by jun on 08/01/16.
 */
public class ThreadPoolTest {
    static class ClickThread implements Runnable {

        @Override
        public void run() {

        }
    }

    public static void main(String[] args) throws InterruptedException {
        ExecutorService cachedPool = Executors.newFixedThreadPool(5000);
        for (int i = 0; i < 10000; i++) {
            cachedPool.submit(new ClickThread());
            if (i % 100 == 0) {
                System.out.println(i);
            }
        }
    }

}
