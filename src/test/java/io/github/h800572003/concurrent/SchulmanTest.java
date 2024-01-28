package io.github.h800572003.concurrent;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.concurrent.*;

@Slf4j
public class SchulmanTest {


    @Test
    void test(){
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(2);


        ScheduledFuture<?> scheduledFuture = scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {

                log.info("hello start...");
                try {
                    TimeUnit.SECONDS.sleep(3);
                } catch (InterruptedException e) {
                    log.info("InterruptedException");
                }finally {
                    close();
                }

            }

            private void close() {
                try {
                    log.info("close...start");
                    TimeUnit.SECONDS.sleep(3);
                    log.info("close...end");
                } catch (InterruptedException ex) {

                }
            }
        }, 1, 3, TimeUnit.SECONDS);

        scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {

                boolean cancel = scheduledFuture.cancel(false);
                log.info("call {}",cancel);
                try {
                    scheduledFuture.get();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
        }, 3, TimeUnit.SECONDS);


        try {
            TimeUnit.SECONDS.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }
}
