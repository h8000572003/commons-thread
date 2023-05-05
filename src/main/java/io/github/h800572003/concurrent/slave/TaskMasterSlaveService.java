package io.github.h800572003.concurrent.slave;

import com.google.common.base.Stopwatch;
import io.github.h800572003.concurrent.ConcurrentException;
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

    private final String masterPrefix ;
    private final String slavePrefix;



    /**
     * 單一主線路服務
     *
     * @param slaveStartSec
     * @param slaveSize
     * @param closeTimeoutSec 關閉等待秒數
     * @return
     */
    public static TaskMasterSlaveService newSingleCore(long slaveStartSec, int slaveSize, int closeTimeoutSec) {
        return new TaskMasterSlaveService(1, slaveStartSec, slaveSize, closeTimeoutSec, CORE_PATTEN,SLAVE_PATTEN);
    }


    public TaskMasterSlaveService(int coreSize, long slaveStartSec, int slaveSize) {
        this(coreSize, slaveStartSec, slaveSize, DEFAULT_CLOSE_TIMEOUT, CORE_PATTEN,SLAVE_PATTEN);
    }

    /**
     *
     * @param coreSize 主奴隸數量
     * @param slaveStartSec 支援奴隸啟動秒數
     * @param slaveSize 支援奴隸數量
     * @param closeTimeout 關閉任務timeout時間
     * @param masterPrefix 主奴隸名稱
     * @param slavePrefix 支援奴隸名稱
     */
    public TaskMasterSlaveService(int coreSize,//
                                  long slaveStartSec,//
                                  int slaveSize,//
                                  int closeTimeout,
                                  final String masterPrefix,//
                                  final String slavePrefix
                                  ) {
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

    public <T> TaskMasterSlaveClient<T> getClient(TaskHandle<T> task, List<T> data) {
        return
                new TaskMasterSlaveClient<>(//
                        data,//
                        task//
                );//

    }


    /**
     * 啟動
     *
     * @param task
     * @param data
     * @param <T>
     */
    public <T> void start(TaskHandle<T> task, List<T> data) {
        this.getClient(task, data).start();
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
    public class TaskMasterSlaveClient<T> {
        final ScheduledExecutorService master;//主要任務

        final ScheduledExecutorService slave;//奴隸服務



        private final List<T> all;
        private final List<T> ok = new CopyOnWriteArrayList<>();
        private final List<T> error = new CopyOnWriteArrayList<>();

        private final BlockingQueue<T> queue;


        private CountDownLatch taskLatch;//任務栓


        private TaskHandle<T> task;//任務處理


        private List<TaskMasterSlaveObserver<T>> observers = new ArrayList<>();

        private AtomicBoolean isRunning = new AtomicBoolean(true);


        public TaskMasterSlaveClient(List<T> data, TaskHandle<T> task) {
            this.all = data;
            this.master = new ScheduledThreadPoolExecutor(coreSize, new CustomizableThreadFactory(masterPrefix));
            this.slave = new ScheduledThreadPoolExecutor(slaveSize, new CustomizableThreadFactory(slavePrefix));
            this.taskLatch = new CountDownLatch(data.size());
            this.queue = new LinkedBlockingQueue<>(data);
            this.task = task;
        }


        public void addRegister(TaskMasterSlaveObserver<T> observer) {
            this.observers.add(observer);
        }


        /**
         * 啟動
         */
        public void start() {
            if (!queue.isEmpty()) {
                setSlaveStartSec();
                addCoreWork();
            }
            try {
                this.taskLatch.await();
            } catch (InterruptedException e) {
                log.info("get InterruptedException");
                Thread.currentThread().interrupt();
            } finally {
                shutdown();
            }
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
                this.slave.execute(this::execute);
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


        private void execute() {
            T data = null;
            while (!queue.isEmpty() && !Thread.interrupted() && isRunning.get()) {
                try {
                    data = queue.take();
                    this.handle(data);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            log.trace("work recycle..");
        }


        /**
         * 設定奴隸啟動秒數
         */
        private void setSlaveStartSec() {
            this.slave.schedule(this::startSlave, slaveStartSec, TimeUnit.SECONDS);
        }

        /**
         * 時間到啟動服務
         */
        private void startSlave() {
            addSlave();
            observers.forEach(TaskMasterSlaveObserver::updateOpenSlave);
        }

        public void handle(T t)  {
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
