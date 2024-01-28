package io.github.h800572003.concurrent.slave;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.RepeatedTest;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
class TestTaskMasterSlaveServiceTest {

    @RepeatedTest(10)
    void testLoopTest() {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        int coreSize = random.nextInt(1, 4);
        int slaveSize = random.nextInt(1, 4);
        log.info("coreSize size: " + coreSize + " slave size: " + slaveSize);
        TestTaskMasterSlaveService testTaskMasterSlaveService = new TestTaskMasterSlaveService(coreSize,
                3,//
                slaveSize,//
                3,//
                2);//
        testTaskMasterSlaveService.test();
    }

    @RepeatedTest(10)
    void testNotData() {

        ThreadLocalRandom random = ThreadLocalRandom.current();

        int coreSize = random.nextInt(1, 4);
        int slaveSize = random.nextInt(1, 4);
        TestTaskMasterSlaveService testTaskMasterSlaveService = new TestTaskMasterSlaveService(coreSize,
                0,
                slaveSize,
                10,
                0);
        testTaskMasterSlaveService.test();

        log.info("coreSize:{} slaveSize:{} recycle:{}", coreSize, slaveSize, testTaskMasterSlaveService.getThreadSet().size());
        Assertions.assertTrue(testTaskMasterSlaveService.getThreadSet().size() <= coreSize + slaveSize);
    }

    @RepeatedTest(10)
    void testWithInterrupted() throws InterruptedException {
        AtomicReference<TestTaskMasterSlaveService> testTaskMasterSlaveService = new AtomicReference<>();
        int threadSize = 4;
        int loopTime = 1;

        AtomicInteger InterruptedCounter = new AtomicInteger();


        Thread thread = getThread(testTaskMasterSlaveService, threadSize, loopTime, InterruptedCounter);
        TimeUnit.MILLISECONDS.sleep(ThreadLocalRandom.current().nextInt(1, 1000));
        thread.interrupt();
        log.info("interruptWhenInBlock...");

        thread.join();

        testTaskMasterSlaveService.get().isAllThreadTerminated();
        log.info("startCounter:{} okCounter:{} errorCounter:{}",
                testTaskMasterSlaveService.get().getStartCounter(),
                testTaskMasterSlaveService.get().getOkCounter(),
                testTaskMasterSlaveService.get().getErrorCounter());
//        Assertions.assertEquals(testTaskMasterSlaveService.get().getStartCounter().get(), testTaskMasterSlaveService.get().getOkCounter().get());

        Assertions.assertEquals(testTaskMasterSlaveService.get().getStartCounter().get(),
                testTaskMasterSlaveService.get().getErrorCounter().get()
                        + testTaskMasterSlaveService.get().getOkCounter().get());

        log.info("finish");

    }

    private static Thread getThread(AtomicReference<TestTaskMasterSlaveService> testTaskMasterSlaveService, int threadSize, int loopTime, AtomicInteger interruptedCounter) {

        Thread thread = new Thread(() -> {
            testTaskMasterSlaveService.set(new TestTaskMasterSlaveService(threadSize,
                    1,
                    threadSize,
                    loopTime,
                    10));
            testTaskMasterSlaveService.get().execute(() -> {
                try {
                    log.info("TestTaskMasterSlaveService started");
                    TimeUnit.MILLISECONDS.sleep(ThreadLocalRandom.current().nextInt(1, 100));
                } catch (InterruptedException e) {
                    interruptedCounter.incrementAndGet();
                    log.info("TestTaskMasterSlaveService with interrupted exception");
                    throw new RuntimeException(e);
                } finally {
                    log.info("TestTaskMasterSlaveService end");
                }
            });
        });
        thread.start();
        return thread;
    }
}