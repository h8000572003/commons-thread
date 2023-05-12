package io.github.h800572003.concurrent.slave;

import com.google.common.base.Stopwatch;
import io.github.h800572003.concurrent.ConcurrentException;
import io.github.h800572003.concurrent.IBlockKey;
import io.github.h800572003.concurrent.OrderQueue;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 任務主奴隸服務
 */
@Slf4j
public class TaskMasterSlaveService {

    public static final int DEFAULT_CLOSE_TIMEOUT = 60;
    private final int closeTimeout;


    public static final String CORE_PATTEN = "core_";
    public static final String SLAVE_PATTEN = "slave_";
    private final long slaveStartSec;//
    private final int slaveSize;//奴隸尺寸
    private final int coreSize;//核心尺寸

    private String masterPrefix = "";
    private String slavePrefix = "";

    /**
     * 單一主線路服務
     *
     * @param slaveStartSec
     * @param slaveSize
     * @param closeTimeoutSec 關閉等待秒數
     * @return
     */
    public static TaskMasterSlaveService newSingleCore(long slaveStartSec, int slaveSize, int closeTimeoutSec) {
        return new TaskMasterSlaveService(1, slaveStartSec, slaveSize, closeTimeoutSec, CORE_PATTEN, SLAVE_PATTEN);
    }


    public TaskMasterSlaveService(int coreSize, long slaveStartSec, int slaveSize) {
        this(coreSize, slaveStartSec, slaveSize, DEFAULT_CLOSE_TIMEOUT, CORE_PATTEN, SLAVE_PATTEN);
    }

    /**
     * @param coreSize      核心數量
     * @param slaveStartSec 奴隸啟動時間
     * @param slaveSize     奴隸數量
     */
    public TaskMasterSlaveService(int coreSize,//
                                  long slaveStartSec,//
                                  int slaveSize,//
                                  int closeTimeout,
                                  final String masterPrefix,//
                                  final String slavePrefix) {
        this.masterPrefix = masterPrefix;
        this.slavePrefix = slavePrefix;
        this.coreSize = coreSize;
        this.closeTimeout = closeTimeout;
        this.slaveStartSec = slaveStartSec;
        this.slaveSize = slaveSize;
        if (this.slaveSize <= 0) {
            throw new ConcurrentException("work size more than 0");
        }
        if (this.slaveStartSec <= 0) {
            throw new ConcurrentException("timeout size more than 0");
        }
    }

    public <T extends IBlockKey> TaskMasterSlaveClient<T> getClient(TaskHandle<T> task, List<T> data) {
        return
                getClient(task, data, new OrderQueue<T>());

    }

    public <T extends IBlockKey> TaskMasterSlaveClient<T> getClient(TaskHandle<T> task, List<T> data, OrderQueue<T> queue) {
        return
                new TaskMasterSlaveClient<>(//
                        data,//
                        task,//
                        queue
                );//

    }

    /**
     * 啟動
     *
     * @param task
     * @param data
     * @param <T>
     */
    public <T extends IBlockKey> void start(TaskHandle<T> task, List<T> data, OrderQueue<T> queue) {
        this.getClient(task, data, queue).run();
    }

    /**
     * 啟動
     *
     * @param task
     * @param data
     * @param <T>
     */
    public <T extends IBlockKey> void start(TaskHandle<T> task, List<T> data) {
        this.getClient(task, data).run();
    }


    /**
     * 任務觀察者
     */
    public interface TaskMasterSlaveObserver<T> {

        /**
         * 通知啟動通知
         */
        default void updateOpenSlave() {

        }

        /**
         * 通知
         *
         * @param total
         * @param ok
         * @param error
         */
        default void updateClose(List<T> total, List<T> ok, List<T> error) {

        }

        default void updateInterrupted() {

        }

        default void updateError(T data, Throwable throwable) {

        }
    }


    /**
     * client
     *
     * @param <T>
     */
    public class TaskMasterSlaveClient<T extends IBlockKey> {
        final ScheduledExecutorService master;//主要任務

        final ScheduledExecutorService slave;//奴隸服務

        final ScheduledExecutorService slaveStartService;//奴隸啟動排程


        private final List<T> all;
        private final List<T> ok = new CopyOnWriteArrayList<>();
        private final List<T> error = new CopyOnWriteArrayList<>();

        private final OrderQueue<T> queue;


        private CountDownLatch taskLatch;//任務栓


        private TaskHandle<T> task;//任務處理

        private CountDownLatch slaveLatch = new CountDownLatch(1);//奴隸開關

        private List<TaskMasterSlaveObserver<T>> observers = new ArrayList<>();

        private AtomicBoolean isRunning = new AtomicBoolean(true);


        public TaskMasterSlaveClient(List<T> data, TaskHandle<T> task, OrderQueue<T> queue) {
            this.all = data;
            this.master = new ScheduledThreadPoolExecutor(coreSize, new CustomizableThreadFactory(masterPrefix));
            this.slave = new ScheduledThreadPoolExecutor(slaveSize, new CustomizableThreadFactory(slavePrefix));
            this.slaveStartService = new ScheduledThreadPoolExecutor(1, new CustomizableThreadFactory("slave_time_"));
            this.taskLatch = new CountDownLatch(data.size());
            this.queue = queue;
            this.task = task;

        }


        public void addRegister(TaskMasterSlaveObserver<T> observer) {
            this.observers.add(observer);
        }

        /**
         * 啟動
         */
        public void run() {
            addProducer();
            setSlaveStartSec();
            addCoreWork();
            addSlave();
            try {
                this.taskLatch.await();
            } catch (InterruptedException e) {
                log.info("get InterruptedException");
                Thread.currentThread().interrupt();
            } finally {
                shutdown();
            }
        }

        private void addProducer() {
            Thread thread = new Thread(() -> {
                for (T t : all) {
                    this.queue.add(t);
                }
            });
            thread.start();
        }

        private void shutdown() {
            try {
                if (Thread.interrupted()) {
                    observers.forEach(TaskMasterSlaveObserver::updateInterrupted);
                    this.isRunning.set(false);
                    try {
                        log.info("wait..start");
                        getScheduledExecutorService().forEach(i -> this.close(i::shutdown));
                        getScheduledExecutorService().forEach(this::awaitTermination);
                    } finally {
                        log.info("wait..end");
                    }
                }
            } finally {
                this.close(this.master::shutdownNow);
                this.close(this.slave::shutdownNow);
                this.close(this.slaveStartService::shutdownNow);

                this.observers.forEach(i -> i.updateClose(this.all, this.ok, this.error));
            }


        }

        private void awaitTermination(final ScheduledExecutorService scheduledExecutorService) {
            int waitSect = 0;
            while (true) {
                try {
                    while (!scheduledExecutorService.awaitTermination(1, TimeUnit.SECONDS) && waitSect++ <= closeTimeout) {
                        log.debug("wait..{}", waitSect);
                    }
                    break;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

        }

        private void close(Runnable runnable) {
            try {
                runnable.run();
            } catch (Exception e) {
                log.error("e", e);
            }
        }

        private void addSlave() {
            for (int i = 0; i < slaveSize; i++) {
                this.slave.execute(this::executeSlave);
            }
        }


        /**
         * 加入核心工人數
         */
        private void addCoreWork() {
            for (int i = 0; i < coreSize; i++) {
                this.master.execute(this::execute);
            }

        }

        private void executeSlave() {
            try {
                slaveLatch.await();
                this.execute();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }


        }

        private void execute() {
            T data = null;
            while (!Thread.currentThread().isInterrupted() && isRunning.get()) {
                try {
                    data = queue.take();
                    this.handle(data);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    queue.remove(data);
                }
            }
            log.trace("work recycle..");
        }


        /**
         * 設定奴隸啟動秒數
         */
        private void setSlaveStartSec() {
            this.slaveStartService.schedule(() -> {
                        slaveLatch.countDown();
                        observers.forEach(TaskMasterSlaveObserver::updateOpenSlave);
                    }
                    , slaveStartSec, TimeUnit.SECONDS);
        }

        public void handle(T t) throws InterruptedException {
            Stopwatch stopwatch = Stopwatch.createStarted();
            try {
                log.trace("start src {} handle done", t);
                task.handle(t);
                ok.add(t);
            } catch (Exception e) {
                error.add(t);
                this.observers.forEach(i -> i.updateError(t, e));
            } finally {
                stopwatch.stop();
                log.trace("finally src {} handle done spent:{}ms ", t, stopwatch.elapsed(TimeUnit.MILLISECONDS));
                taskLatch.countDown();
            }
        }

        private List<ScheduledExecutorService> getScheduledExecutorService() {
            List<ScheduledExecutorService> scheduledExecutorServices = new ArrayList<>();
            if (master != null) {
                scheduledExecutorServices.add(master);
            }
            if (slave != null) {
                scheduledExecutorServices.add(slave);
            }
            if (slaveStartService != null) {
                scheduledExecutorServices.add(slaveStartService);
            }
            return scheduledExecutorServices;


        }
    }


    /**
     * 任務處理
     *
     * @param <T>
     */
    @FunctionalInterface
    public interface TaskHandle<T> {
        void handle(T data);
    }
}
