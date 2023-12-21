package io.github.h800572003.concurrent;


import io.github.h800572003.concurrent.slave.TaskMasterSlaveService;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

@Slf4j
class TaskMasterSlaveServiceTest {


    /**
     * 測試工人數量大於任務數，且任務屬於長時間任務
     * give core worker :2
     * and slave worker:3
     * and 停止任務 不強制中斷 5s
     * and 若分配下工作等無限時間
     * and wait 5s
     * <p>
     * when執行任務
     * and 5秒鐘中斷
     * <p>
     * then
     * give ok item < total tasks
     * but
     * worker recycle count
     */
    @Test
    @Disabled
    void testLessTask() throws InterruptedException {
        final Task task = new Task();

        //give task size {int}
        final List<BlockItem> collect = LongStream.range(20, 22).boxed().map(BlockItem::new).collect(Collectors.toList());


        final List<Thread> workers = new CopyOnWriteArrayList<>();
        Thread thread = getThread(task, collect, workers, 2);
        TimeUnit.SECONDS.sleep(5);
        thread.interrupt();
        thread.join();

        log.info("alive value:{}", task.alive.get());
        log.info("start value:{}", task.startValue.get());
        log.info("endValue value:{}", task.endValue.get());
        log.info("interruptedExceptionValue value:{}", task.interruptedExceptionValue.get());
        log.info("worker size:{}", workers.size());
    }

    /**
     * 測試工人數量小於任務數，且任務屬於長時間任務
     * give 給100無法完成總數任務
     * and core worker :2
     * and slave worker:3
     * and wait 5s
     * and 停止任務 不強制中斷5秒
     * and 若分配下工作等無限時間
     * <p>
     * when執行任務
     * and 5秒鐘中斷
     * <p>
     * then
     * give ok item < total tasks
     * but
     * worker recycle count
     */
    @Test
    @Disabled
    void testLongTimeTask() throws InterruptedException {
        final Task task = new Task();

        //give task size {int}
        final List<BlockItem> collect = LongStream.range(1, 100).boxed().map(BlockItem::new).collect(Collectors.toList());

        //recycle worker
        final List<Thread> workers = new CopyOnWriteArrayList<>();

        Thread thread = getThread(task, collect, workers, 5);
        TimeUnit.SECONDS.sleep(5);
        thread.interrupt();
        thread.join();

        log.info("alive value:{}", task.alive.get());
        log.info("start value:{}", task.startValue.get());
        log.info("endValue value:{}", task.endValue.get());
        log.info("interruptedExceptionValue value:{}", task.interruptedExceptionValue.get());
        log.info("worker size:{}", workers.size());

        Assertions.assertEquals(0, task.alive.get());
        Assertions.assertTrue(collect.size() >= task.startValue.get());
        Assertions.assertTrue(collect.size() >= task.endValue.get());
        Assertions.assertTrue(task.startValue.get() >= task.endValue.get());
        Assertions.assertEquals(5, workers.size());
    }

    private static Thread getThread(Task task, List<BlockItem> collect, List<Thread> workers, int closeTimeout) {
        Thread thread = new Thread(() -> {
            TaskMasterSlaveService service = new TaskMasterSlaveService(2,
                    1, //
                    3, closeTimeout, "master", "slave", -1);

            TaskMasterSlaveService.TaskMasterSlaveClient<BlockItem> client = service.getClient(task, collect);

            client.addRegister(new TaskMasterSlaveService.TaskMasterSlaveObserver<BlockItem>() {
                @Override
                public void updateRecycle(Thread currentTread) {
                    workers.add(currentTread);
                }
            });
            client.run();

        });
        thread.start();
        return thread;
    }

    static class Task implements TaskMasterSlaveService.TaskHandle<BlockItem> {

        private final AtomicInteger startValue = new AtomicInteger(0);
        private final AtomicInteger endValue = new AtomicInteger(0);
        private final AtomicInteger interruptedExceptionValue = new AtomicInteger(0);

        private final AtomicInteger alive = new AtomicInteger(0);

        @Override
        public void handle(BlockItem data) {
            startValue.incrementAndGet();
            alive.incrementAndGet();
            log.info(data.getValue() + " start task");
            try {
                TimeUnit.SECONDS.sleep(data.getValue());
            } catch (InterruptedException e) {
                interruptedExceptionValue.incrementAndGet();
                log.info(data.getValue() + " InterruptedException");
            } finally {
                alive.decrementAndGet();
                closeTask(data);
                log.info(data.getValue() + " close task");
                endValue.incrementAndGet();
            }
        }

        private void closeTask(BlockItem data) {
            try {
                log.info(data.getValue() + " close wait....");
                TimeUnit.SECONDS.sleep(data.getValue());
            } catch (InterruptedException ex) {
//                    throw new RuntimeException(ex);
            }
        }
    }


    /**
     * 未啟動Slave
     */
    @Test
    void testMaster() {
        TaskMasterSlaveService service = new TaskMasterSlaveService(2,
                1, //
                3);
        SpyTask task = Mockito.spy(new SpyTask());

        List<BlockItem> collect = Stream.of(1L, 2L, 3L, 4L)
                .map(BlockItem::new)
                .collect(Collectors.toList());

        service.start(task, collect);
        log.info("end");

        Mockito.verify(task, Mockito.times(4)).handle(Mockito.any());

    }

    @Test
    void testSlave() {
        TaskMasterSlaveService service = new TaskMasterSlaveService(2,
                1, //
                3);
        SpyTask task = Mockito.spy(new SpyTask());

        List<BlockItem> longs = Stream.of(1001L, 1002L, 1003L, 1004L)
                .map(BlockItem::new)
                .collect(Collectors.toList());


        TaskMasterSlaveService.TaskMasterSlaveObserver observer = Mockito.spy(new TaskMasterSlaveService.TaskMasterSlaveObserver() {

            @Override
            public void updateClose(List total, List ok, List error) {
                log.info("total:{} ok:{} error:{}", total.size(), ok.size(), error.size());
                Assertions.assertEquals(4, total.size());
                Assertions.assertEquals(4, ok.size());
                Assertions.assertEquals(0, error.size());

            }

            @Override
            public void updateOpenSlave() {
                log.info("updateOpenSlave..");
            }

            @Override
            public void updateInterrupted() {
                log.info("updateInterrupted..");
            }
        });

        TaskMasterSlaveService.TaskMasterSlaveClient client = service.getClient(task, longs);
        client.addRegister(observer);
        client.run();


        Mockito.verify(task, Mockito.times(4)).handle(Mockito.any());

    }

    /**
     * GIVE 沒有執行
     * WHEN 執行時
     * THEN 結束作業並且不呼叫任務
     */
    @Test
    @Timeout(2)
    void testNoTask() {
        TaskMasterSlaveService service = new TaskMasterSlaveService(2,
                1, //
                3);
        SpyTask task = Mockito.spy(new SpyTask());


        List<BlockItem> longs = Collections.emptyList();
        service.start(task, longs);
        log.info("end");

        Mockito.verify(task, Mockito.times(0)).handle(Mockito.any());


    }

    /**
     * 測試相同key的不可同時開始
     */
    @Test
    void testSameKey() {

        SpyTask task = Mockito.spy(new SpyTask());
        TaskMasterSlaveService service = new TaskMasterSlaveService(2,
                1, //
                3);

        List<BlockItem> longs = Stream.of(1000L, 1000L, 1000L, 1000L, 1000L)
                .map(BlockItem::new)
                .collect(Collectors.toList());

        service.start(task, longs);

        Mockito.verify(task, Mockito.times(5)).handle(Mockito.any());


    }


    /**
     * GIVE第四筆案例會發生程式中斷，中斷後
     * WHEN 執行時
     * THEN 進行通知，並完成所有作業
     */
    @Test
    @Timeout(2)
    void testNoTaskWitError() {

        ArgumentCaptor<List<Long>> errorTask = ArgumentCaptor.forClass(List.class);
        TaskMasterSlaveService.TaskMasterSlaveObserver observer = Mockito.spy(new TaskMasterSlaveService.TaskMasterSlaveObserver() {
            @Override
            public void updateError(Object data, Throwable throwable) {
                log.error("data:{}", data, throwable);
            }
        });

        TaskMasterSlaveService service = new TaskMasterSlaveService(1,
                1, //
                3);

        int errorIndex = 2;

        SpyTask task = Mockito.spy(new SpyTask(errorIndex));


        List<BlockItem> longs = Stream.of(101L, 102L, 103L, 104L, 105L)
                .map(BlockItem::new)
                .collect(Collectors.toList());

        TaskMasterSlaveService.TaskMasterSlaveClient<BlockItem> client = service.getClient(task, longs);
        client.addRegister(observer);
        client.run();
        log.info("end");

        Mockito.verify(observer, Mockito.times(1)).updateError(Mockito.eq(longs.get(errorIndex - 1)), Mockito.any());//檢查發生錯誤該筆是否正確
        Mockito.verify(task, Mockito.times(5)).handle(Mockito.any());//依舊執行五次
        Mockito.verify(observer, Mockito.times(1)).updateClose(Mockito.anyList(), Mockito.anyList(), errorTask.capture());


        //有錯誤
        Assertions.assertEquals(1, errorTask.getValue().size());
    }


   static class SpyTask implements TaskMasterSlaveService.TaskHandle<BlockItem> {

        private final int errorSize;


        public SpyTask(int errorSize) {
            this.errorSize = errorSize;
        }

        public SpyTask() {
            this(-1);
        }

        private final CopyOnWriteArrayList copyOnWriteArrayList = new CopyOnWriteArrayList();

        @Override
        public void handle(BlockItem item) {


            Long value = item.getValue();
            copyOnWriteArrayList.add(value);
            log.info(" start execute workTime:{}", value);
            try {
                TimeUnit.MILLISECONDS.sleep(value);
                if (copyOnWriteArrayList.size() == errorSize) {
                    throw new RuntimeException("發生異常");
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                log.info(" end execute workTime:{}", value);


            }

        }


    }


    @Getter
    static class BlockItem implements IBlockKey {

        private final Long value;

        public BlockItem(Long aLong) {
            this.value = aLong;
        }

        @Override
        public String toKey() {
            return value.toString();
        }

    }

}