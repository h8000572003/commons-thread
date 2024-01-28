package io.github.h800572003.concurrent.slave;

import io.github.h800572003.concurrent.IBlockKey;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.junit.jupiter.api.Assertions;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
class TestTaskMasterSlaveService {
    private final int coreSize;
    private final int slaveSize;
    private final long slaveStartSec;
    private final int loopTime;
    private final int timeEachLoop;
    private TaskMasterSlaveService service;


    private final AtomicInteger times = new AtomicInteger();

    @Getter
    private final Set<Thread> threadSet = new CopyOnWriteArraySet<>();

    @Setter
    private boolean withLoopingTimeAssert = true;


    private boolean withShutdownAssert = true;

    private boolean withCounterAssert = true;

    @Getter
    private final AtomicInteger startCounter = new AtomicInteger(0);

    @Getter
    private final AtomicInteger okCounter = new AtomicInteger(0);

    @Getter
    private final AtomicInteger errorCounter = new AtomicInteger(0);


    @Getter
    private List<TaskMasterSlaveService.TaskMasterSlaveClient<BlockItem>> clients = new CopyOnWriteArrayList<>();


    public TestTaskMasterSlaveService(int coreSize, long slaveStartSec, int slaveSize, int loopTime, int timeEachLoop) {
        this.coreSize = coreSize;
        this.slaveSize = slaveSize;
        this.slaveStartSec = slaveStartSec;
        this.loopTime = loopTime;
        this.timeEachLoop = timeEachLoop;

    }

    void test() {
        this.test(() -> {
        });
    }

    void test(Runnable runnable) {
        execute(runnable);
        forAssertion();
    }

    void execute(Runnable runnable) {
        for (int i = 0; i < loopTime; i++) {

            this.service = new TaskMasterSlaveService(coreSize, slaveStartSec, slaveSize);


            List<BlockItem> blockItemList = IntStream.range(0, timeEachLoop)//
                    .mapToObj(BlockItem::new)//
                    .collect(Collectors.toList());//

            TaskMasterSlaveService.TaskMasterSlaveClient<BlockItem> client = this.service.getClient(data -> {

                try {
                    startCounter.incrementAndGet();
                    log.info("start thread name:{} index:{} value:{}", Thread.currentThread().getName(), data.value, data.key);
                    runnable.run();
                    okCounter.incrementAndGet();
                } catch (Exception e) {
                    errorCounter.incrementAndGet();
                    throw new RuntimeException(e);
                } finally {
                    times.incrementAndGet();
                    log.info("end thread name:{} index:{} value:{}", Thread.currentThread().getName(), data.value, data.key);
                }

            }, blockItemList);

            clients.add(client);
            client.addRegister(new TaskMasterSlaveService.TaskMasterSlaveObserver<BlockItem>() {
                @Override
                public void updateRecycle(Thread currentTread) {
                    threadSet.add(currentTread);
                }
            });
            client.run();


        }
    }

    public void forAssertion() {
        if (withLoopingTimeAssert) {
            Assertions.assertEquals(loopTime * timeEachLoop, times.get());
        }

        if (withShutdownAssert) {
            Assertions.assertTrue(this.isAllThreadTerminated());
        }
        if (withCounterAssert) {
            Assertions.assertEquals(loopTime * timeEachLoop, startCounter.get());
            Assertions.assertEquals(0, errorCounter.get());
            Assertions.assertEquals(loopTime * timeEachLoop, okCounter.get());
        }
    }

    public boolean isAllThreadTerminated() {
        EqualsBuilder equalsBuilder = new EqualsBuilder();
        for (TaskMasterSlaveService.TaskMasterSlaveClient<BlockItem> client : clients) {
            equalsBuilder.append(true, client.isTerminated());
            equalsBuilder.append(true, client.isShutdown());
        }
        return equalsBuilder.isEquals();
    }

    public int getThreadCount() {
        int sum = 0;
        for (TaskMasterSlaveService.TaskMasterSlaveClient<BlockItem> client : clients) {
            List<TaskMasterSlaveService.Worker<BlockItem>> workers = client.getWorkers();
            for (TaskMasterSlaveService.Worker<BlockItem> worker : workers) {
                if (StringUtils.isNotBlank(worker.getName())) {
                    sum++;
                }
            }
        }
        return sum;
    }


    @Getter
    static class BlockItem implements IBlockKey {

        private final int value;
        private final int key;

        public BlockItem(int aLong) {
            this.key = ThreadLocalRandom.current().nextInt(1, 100);
            this.value = aLong;
        }

        @Override
        public String toKey() {
            return this.key + "";
        }

    }
}