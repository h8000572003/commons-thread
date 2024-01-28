package io.github.h800572003.concurrent.slave;

import com.google.common.base.Stopwatch;
import io.github.h800572003.concurrent.ConcurrentException;
import io.github.h800572003.concurrent.IBlockKey;
import io.github.h800572003.concurrent.OrderQueue;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 任務主奴隸服務
 */
@Slf4j
public class TaskMasterSlaveService {

    public static final String CORE_PATTEN = "core_";

    private final long slaveStartSec;//
    private final int slaveSize;//奴隸尺寸
    private final int coreSize;//核心尺寸

    private final String masterPrefix;


    private final AtomicBoolean isRunning = new AtomicBoolean(true);

    /**
     * 單一主線路服務
     *
     * @param slaveStartSec
     * @param slaveSize
     * @param closeTimeoutSec 關閉等待秒數
     * @return
     */
    public static TaskMasterSlaveService newSingleCore(long slaveStartSec, int slaveSize, int closeTimeoutSec, int shutdownTimeout) {
        return new TaskMasterSlaveService(1, slaveStartSec, slaveSize, CORE_PATTEN);
    }


    public TaskMasterSlaveService(int coreSize, long slaveStartSec, int slaveSize) {
        this(coreSize, slaveStartSec, slaveSize, CORE_PATTEN);
    }

    public TaskMasterSlaveService(int coreSize, long slaveStartSec, int slaveSize, int shutdownTimeout) {
        this(coreSize, slaveStartSec, slaveSize, CORE_PATTEN);
    }

    /**
     * @param coreSize      主奴隸數量
     * @param slaveStartSec 支援奴隸啟動秒數
     * @param slaveSize     支援奴隸數量
     * @param masterPrefix  主奴隸名稱
     */
    public TaskMasterSlaveService(int coreSize,//
                                  long slaveStartSec,//
                                  int slaveSize,//
                                  final String masterPrefix//
    ) {
        this.masterPrefix = masterPrefix;
        this.coreSize = coreSize;
        this.slaveStartSec = slaveStartSec;
        this.slaveSize = slaveSize;
        if (this.slaveSize <= 0) {
            throw new ConcurrentException("work size more than 0");
        }

    }

    public <T extends IBlockKey> TaskMasterSlaveClient<T> getClient(TaskHandle<T> task, List<T> data) {
        return
                getClient(task, data, new OrderQueue<>());

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
         * 回收
         *
         * @param currentTread 回收執行緒
         */
        default void updateRecycle(Thread currentTread) {

        }
    }


    /**
     * client
     *
     * @param <T>
     */
    public class TaskMasterSlaveClient<T extends IBlockKey> {
        final ScheduledExecutorService service;//主要任務

        private final OrderQueue<T> queue;

        private final TaskHandle<T> task;//任務處理

        private final List<TaskMasterSlaveObserver<T>> observers = new ArrayList<>();

        private final ExecutorCompletionService<String> completionService;
        private final CountDownLatch latch;

        @Getter
        private final List<Worker<T>> workers = new CopyOnWriteArrayList<>();


        public TaskMasterSlaveClient(List<T> data, TaskHandle<T> task, OrderQueue<T> queue) {
            int corePoolSize = Math.min(coreSize + slaveSize, Math.max(data.size(), 1));
            this.service = new ScheduledThreadPoolExecutor(corePoolSize, new CustomizableThreadFactory(masterPrefix));
            this.completionService = new ExecutorCompletionService<>(this.service);
            this.queue = queue;
            this.task = task;
            data.forEach(queue::add);
            this.latch = new CountDownLatch(data.size());
            if (!CollectionUtils.isEmpty(data)) {
                this.addWork();
            }

        }

        public boolean isTerminated() {
            return service.isTerminated();
        }

        public boolean isShutdown() {
            return service.isShutdown();
        }

        public void addRegister(TaskMasterSlaveObserver<T> observer) {
            this.observers.add(observer);
        }


        /**
         * 啟動
         */
        public void run() {
            try {
                latch.await();
            } catch (InterruptedException e) {
                log.info("Interrupted");
            } finally {
                isRunning.set(false);

                service.shutdownNow();
                waitForJob();
            }
        }

        private void waitForJob() {
            try {
                service.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                //忽略

            }
            workers.forEach(i -> log.info("name:{} status:{}", i.name, i.status));
        }


//


        /**
         * 加入核心工人數
         */
        private void addWork() {
            for (int i = 0; i < coreSize; i++) {
                final Worker<T> worker = new Worker<>(queue, task, latch, observers);
                workers.add(worker);
                completionService.submit(worker, "");
            }
            for (int i = 0; i < slaveSize; i++) {
                final WorkerSlave<T> worker = new WorkerSlave<>(queue, task, latch, observers);
                workers.add(worker);
                completionService.submit(worker, "");
            }
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

    enum WorkerStatus {
        WAIT_START,//待開始
        RUNNING,//處理中
        WAIT_QUEUED,//待

        RECYCLE,//已回收

        NONE,//未
    }


    class WorkerSlave<T extends IBlockKey> extends Worker<T> {

        public WorkerSlave(OrderQueue<T> queue, TaskHandle<T> task, CountDownLatch latch, List<TaskMasterSlaveObserver<T>> taskMasterSlaveObservers) {
            super(queue, task, latch, taskMasterSlaveObservers);
        }

        @Override
        public void run() {
            try {
                TimeUnit.SECONDS.sleep(slaveStartSec);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }finally {
                super.run();
            }

        }
    }

    class Worker<T extends IBlockKey> implements Runnable {
        private final AtomicReference<WorkerStatus> status = new AtomicReference<>();
        private final OrderQueue<T> queue;

        private final TaskHandle<T> task;//任務處理

        private final CountDownLatch latch;

        private final List<TaskMasterSlaveObserver<T>> observers;


        @Getter
        private String name;

        private Thread thread;

        public Worker(OrderQueue<T> queue, TaskHandle<T> task, CountDownLatch latch, List<TaskMasterSlaveObserver<T>> observers) {
            this.queue = queue;
            this.task = task;
            this.latch = latch;
            this.observers = observers;
            this.status.set(WorkerStatus.NONE);
        }


        @Override
        public void run() {
            try {
                this.thread = Thread.currentThread();
                this.name = Thread.currentThread().getName();
                status.set(WorkerStatus.WAIT_START);
                T data = null;
                while (isRunning.get()) {
                    status.set(WorkerStatus.WAIT_QUEUED);
                    try {
                        data = queue.take();
                        status.set(WorkerStatus.RUNNING);
                        this.handle(data);
                    } catch (InterruptedException e) {
                        log.info("{} Interrupted:" + Thread.currentThread().getName(), isRunning.get());
                        Thread.currentThread().interrupt();
                    } finally {
                        latch.countDown();
                        if (data != null) {
                            queue.remove(data);
                        }
                    }
                }

            } finally {
                this.observers.forEach(i -> i.updateRecycle(Thread.currentThread()));
                status.set(WorkerStatus.RECYCLE);
            }
        }

        public void handle(T t) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            try {
                task.handle(t);
            } catch (Exception e) {
                throw new ConcurrentException("error key:" + t.toKey(), e);
            } finally {
                stopwatch.stop();
                log.trace("finally src {} handle done spent:{}ms ", t, stopwatch.elapsed(TimeUnit.MILLISECONDS));
            }
        }
    }
}
