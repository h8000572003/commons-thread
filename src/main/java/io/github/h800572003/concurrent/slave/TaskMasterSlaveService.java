package io.github.h800572003.concurrent.slave;

import io.github.h800572003.concurrent.ConcurrentException;

import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * 任務主奴隸服務
 */
@Slf4j
public class TaskMasterSlaveService {


    public static final String MASTER_ = "master_";
    public static final String SLAVE_ = "slave_";
    private final long slaveStartSec;//
    private final int slaveSize;//奴隸尺寸
    private final int coreSize;//核心尺寸

    private String masterPrefix = "";
    private String slavePrefix = "";


    public TaskMasterSlaveService(int coreSize, long slaveStartSec, int slaveSize){
        this(coreSize,slaveStartSec,slaveSize, MASTER_, SLAVE_);
    }

    /**
     * @param coreSize      核心數量
     * @param slaveStartSec 奴隸啟動時間
     * @param slaveSize     奴隸數量
     */
    public TaskMasterSlaveService(int coreSize,//
                                  long slaveStartSec,//
                                  int slaveSize,//
                                  final String masterPrefix,//
                                  final String slavePrefix ) {
        this.masterPrefix=masterPrefix;
        this.slavePrefix=slavePrefix;
        this.coreSize = coreSize;
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
    }


    /**
     * client
     * @param <T>
     */
    public class TaskMasterSlaveClient<T> {
        final ScheduledExecutorService master;//主要任務

        final ScheduledExecutorService slave;//奴隸服務

        final ScheduledExecutorService slaveStartService;//奴隸啟動排程


        private final List<T> all;
        private final List<T> ok = new CopyOnWriteArrayList<>();
        private final List<T> error = new CopyOnWriteArrayList<>();

        private final BlockingQueue<T> queue;


        private CountDownLatch taskLatch;//任務栓


        private TaskHandle<T> task;//任務處理

        private CountDownLatch slaveLatch = new CountDownLatch(1);//奴隸開關

        private List<TaskMasterSlaveObserver<T>> observers = new ArrayList<>();


        public TaskMasterSlaveClient(List<T> data, TaskHandle<T> task) {
            this.all = data;
            this.master = new ScheduledThreadPoolExecutor(coreSize, new CustomizableThreadFactory(masterPrefix));
            this.slave = new ScheduledThreadPoolExecutor(slaveSize, new CustomizableThreadFactory(slavePrefix));
            this.slaveStartService = new ScheduledThreadPoolExecutor(1);
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
                addSlave();
            }
            try {
                this.taskLatch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                if (Thread.currentThread().isInterrupted()) {
                    this.close(this.master::shutdownNow);
                    this.close(this.slave::shutdownNow);
                    this.close(this.slaveStartService::shutdownNow);
                }
                this.close(this.master::shutdown);
                this.close(this.slave::shutdown);
                this.close(this.slaveStartService::shutdown);

                this.observers.forEach(i -> i.updateClose(this.all, this.ok, this.error));
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
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            this.execute();

        }

        private void execute() {
            try {
                while (!queue.isEmpty() && !Thread.currentThread().isInterrupted()) {
                    this.handle(queue.take());
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (Thread.currentThread().isInterrupted()) {
                observers.forEach(TaskMasterSlaveObserver::updateInterrupted);
            }
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
            try {
                log.trace("start src {} handle done", t);
                task.handle(t);
                ok.add(t);
            } catch (Exception e) {
                error.add(t);
                throw new ConcurrentException("task:" + t, e);
            } finally {
                log.trace("finally src {} handle done", t);
                taskLatch.countDown();
            }
        }
    }


    /**
     * 任務處理
     * @param <T>
     */
    @FunctionalInterface
    public interface TaskHandle<T> {
        void handle(T data);
    }
}
