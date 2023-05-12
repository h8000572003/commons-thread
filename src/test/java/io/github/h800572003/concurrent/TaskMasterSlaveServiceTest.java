package io.github.h800572003.concurrent;


import io.github.h800572003.concurrent.slave.TaskMasterSlaveService;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.util.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Slf4j
class TaskMasterSlaveServiceTest {


    /**
     * 未啟動Slave
     */
    @Test
    void testMaster() {
        TaskMasterSlaveService service = new TaskMasterSlaveService(2,
                1, //
                3);
        SpyTask task = Mockito.spy(new SpyTask());

        List<BlockItem> collect = Arrays.asList(1L, 2L, 3L, 4L).stream()
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

        List<BlockItem> longs = Arrays.asList(1001l, 1002L, 1003L, 1004L).stream()
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
     * GIVE 三筆資料 一秒後中斷作業
     * WHEN 啟動後
     * THEN 開始執行作業將會完成，其餘的的不執行
     *
     * @throws InterruptedException
     */
    @Disabled("多工可能測試錯誤")
    @Test
    void testInterrupted() throws InterruptedException {

        TaskMasterSlaveService service = TaskMasterSlaveService.newSingleCore(1, 1, 5);

        SpyTask task = Mockito.spy(new SpyTask());

        ArgumentCaptor<List<Long>> errorTask = ArgumentCaptor.forClass(List.class);
        TaskMasterSlaveService.TaskMasterSlaveObserver observer = Mockito.spy(new TaskMasterSlaveService.TaskMasterSlaveObserver() {

            @Override
            public void updateClose(List total, List ok, List error) {
                log.info("total:{} ok:{} error:{}", total.size(), ok.size(), error.size());


            }
        });
        Thread end = new Thread(() -> {

            List<BlockItem> longs = Arrays.asList(1001l, 5001L).stream()
                    .map(BlockItem::new)
                    .collect(Collectors.toList());

            TaskMasterSlaveService.TaskMasterSlaveClient client = service.getClient(task, longs);
            client.addRegister(observer);
            client.run();

        });

        end.start();
        TimeUnit.MILLISECONDS.sleep(1200l);
        end.interrupt();

        end.join();


        Mockito.verify(observer, Mockito.times(1)).updateClose(Mockito.anyList(), Mockito.anyList(), errorTask.capture());

        //沒有錯誤項目
        Assertions.assertEquals(0, errorTask.getValue().size());
    }

    /**
     * GIVE 沒有執行
     * WHEN 執行時
     * THEN 結束作業並且不呼叫任務
     *
     * @throws InterruptedException
     */
    @Test
    @Timeout(2)
    void testNoTask() throws InterruptedException {
        TaskMasterSlaveService service = new TaskMasterSlaveService(2,
                1, //
                3);
        SpyTask task = Mockito.spy(new SpyTask());


        List<BlockItem> longs = Arrays.asList();
        service.start(task, longs);
        log.info("end");

        Mockito.verify(task, Mockito.times(0)).handle(Mockito.any());


    }

    /**
     * 測試相同key的不可同時開始
     */
    @Test
    void testSameKey(){

        SpyTask task = Mockito.spy(new SpyTask());
        TaskMasterSlaveService service = new TaskMasterSlaveService(2,
                1, //
                3);

        List<BlockItem> longs = Arrays.asList(1000L, 1000L,1000L,1000L,1000L).stream()
                .map(BlockItem::new)
                .collect(Collectors.toList());

        service.start(task, longs);

        Mockito.verify(task, Mockito.times(5)).handle(Mockito.any());


    }


    /**
     * GIVE第四筆案例會發生程式中斷，中斷後
     * WHEN 執行時
     * THEN 進行通知，並完成所有作業
     *
     * @throws InterruptedException
     */
    @Test
    @Timeout(2)
    void testNoTaskWitError() throws InterruptedException {

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



        List<BlockItem> longs = Arrays.asList(101L, 102L, 103L, 104L, 105L).stream()
                .map(BlockItem::new)
                .collect(Collectors.toList());

        TaskMasterSlaveService.TaskMasterSlaveClient<BlockItem> client = service.getClient(task, longs);
        client.addRegister(observer);
        client.run();
        log.info("end");

        Mockito.verify(observer, Mockito.times(1)).updateError(Mockito.eq(longs.get(errorIndex - 1)), Mockito.any());//檢查發生錯誤該筆是否正確
        Mockito.verify(task, Mockito.times(5)).handle(Mockito.any());//依舊執行五次
        Mockito.verify(observer, Mockito.times(1)).updateClose(Mockito.anyList(), Mockito.anyList(), errorTask.capture());


        //有一項錯誤
        Assertions.assertEquals(1, errorTask.getValue().size());
    }


    class SpyTask implements TaskMasterSlaveService.TaskHandle<BlockItem> {

        private int errorSize = -1;




        public SpyTask(int errorSize) {
            this.errorSize = errorSize;
        }

        public SpyTask() {
            this(-1);
        }

        private CopyOnWriteArrayList copyOnWriteArrayList = new CopyOnWriteArrayList();

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



    class BlockItem implements IBlockKey{

        private Long value;

        public BlockItem(Long aLong) {
            this.value = aLong;
        }

        @Override
        public String toKey() {
            return value.toString();
        }

        public Long getValue() {
            return value;
        }
    }

}