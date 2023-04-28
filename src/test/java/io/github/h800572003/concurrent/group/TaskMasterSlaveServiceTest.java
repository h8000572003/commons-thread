package io.github.h800572003.concurrent.group;


import io.github.h800572003.concurrent.slave.TaskMasterSlaveService;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
class TaskMasterSlaveServiceTest {


    private TaskMasterSlaveService service = new TaskMasterSlaveService(1,
            1, //
            3);

    /**
     * 未啟動Slave
     */
    @Test
    void testMaster() {
        SpyTask task = Mockito.spy(new SpyTask());

        List<Long> longs = Arrays.asList(1L, 1L, 1L, 1L);
        service.start(task, longs);
        log.info("end");

        Mockito.verify(task, Mockito.times(4)).handle(Mockito.anyLong());

    }

    @Test
    @Timeout(3)
    void testSlave() {
        SpyTask task = Mockito.spy(new SpyTask());

        List<Long> longs = Arrays.asList(1001l, 1002L, 1003L, 1004L);


        TaskMasterSlaveService.TaskMasterSlaveObserver observer = Mockito.spy(new TaskMasterSlaveService.TaskMasterSlaveObserver() {

            @Override
            public void updateClose(List total, List ok, List error) {
                log.info("total:{} ok:{} error:{}",total.size(),ok.size(),error.size());
                Assertions.assertEquals(4,total.size());
                Assertions.assertEquals(4,ok.size());
                Assertions.assertEquals(0,error.size());

            }
        });

        TaskMasterSlaveService.TaskMasterSlaveClient client = service.getClient(task, longs);
        client.addRegister(observer);
        client.start();


        Mockito.verify(task, Mockito.times(4)).handle(Mockito.anyLong());

        Mockito.verify(observer, Mockito.times(1)).updateClose(Mockito.anyList(),Mockito.anyList(),Mockito.anyList());
    }

    @Disabled("多工可能測試錯誤")
    @Test
    void testInterrupted() throws InterruptedException {
        SpyTask task = Mockito.spy(new SpyTask());

        TaskMasterSlaveService.TaskMasterSlaveObserver observer = Mockito.spy(new TaskMasterSlaveService.TaskMasterSlaveObserver() {

            @Override
            public void updateClose(List total, List ok, List error) {
                log.info("total:{} ok:{} error:{}",total.size(),ok.size(),error.size());
            }
        });

        Thread end = new Thread(new Runnable() {
            @Override
            public void run() {
                List<Long> longs = Arrays.asList(1001L, 1002L, 1003L, 1004L);
                TaskMasterSlaveService.TaskMasterSlaveClient client = service.getClient(task, longs);
                client.addRegister(observer);
                client.start();

            }
        });
        end.start();
        TimeUnit.MILLISECONDS.sleep(1200l);
        end.interrupt();

        Mockito.verify(observer, Mockito.times(1)).updateClose(Mockito.anyList(), Mockito.anyList(), Mockito.anyList());

        TimeUnit.MILLISECONDS.sleep(300l);
    }

    @Test
    @Timeout(2)
    void testNoTask() throws InterruptedException {
        SpyTask task = Mockito.spy(new SpyTask());

        List<Long> longs = Arrays.asList();
        service.start(task, longs);
        log.info("end");

        Mockito.verify(task, Mockito.times(0)).handle(Mockito.anyLong());


    }

    class SpyTask implements TaskMasterSlaveService.TaskHandle<Long> {

        @Override
        public void handle(Long value) {
            log.info(" start execute workTime:{}", value);
            try {
                TimeUnit.MILLISECONDS.sleep(value);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                log.info(" end execute workTime:{}", value);
            }
        }
    }


}