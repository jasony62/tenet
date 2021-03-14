package io.ctsi.tenet.framework.worker;

import io.ctsi.tenet.kafka.connect.TenetTaskId;
import io.ctsi.tenet.kafka.connect.sink.WorkerTask;
import io.ctsi.tenet.kafka.connect.util.LoggingContext;
import io.ctsi.tenet.kafka.task.TenetWorkerTaskContainer;
import io.ctsi.tenet.kafka.task.util.TenetWorkerTaskConfigUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.SmartLifecycle;
import org.springframework.util.Assert;
import org.springframework.util.ConcurrentReferenceHashMap;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TenetWorkerLifecycle implements ApplicationContextAware, SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(TenetWorkerLifecycle.class);

    private final ExecutorService executor = Executors.newCachedThreadPool();

    private ApplicationContext applicationContext;

    private volatile boolean isRunning = false;

    private TenetWorkerTaskContainer tenetWorkerTaskContainer;

    private TenetThreadPool tenetThreadPool;

    private TenetWorkerTaskContainer.WorkerMetricsGroup workerMetricsGroup;

    private ConcurrentReferenceHashMap<TenetTaskId, WorkerTask> taskContainer;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    public void start() {

        log.info("++++++++ Tenet Worker Lifecycle starting ++++++++");
        /**加载检验bean**/
        tenetWorkerTaskContainer = applicationContext.getBean(TenetWorkerTaskConfigUtil.DEFAULT_TENET_WORKER_TASK_CONTAINER, TenetWorkerTaskContainer.class);
        Assert.state(tenetWorkerTaskContainer!= null, "+++ task start error *tenetWorkerTaskContainer is null object");
        Assert.notEmpty(tenetWorkerTaskContainer.getTaskContainer(), "+++ task start error *tenetWorkerTaskContainer map is empty");
        taskContainer = tenetWorkerTaskContainer.getTaskContainer();
        workerMetricsGroup = tenetWorkerTaskContainer.getWorkerMetricsGroup();
        Assert.state(workerMetricsGroup != null, "+++ task start error *workerMetricsGroup is null object");
        tenetThreadPool = applicationContext.getBean(TenetThreadPool.class);

        Assert.state(tenetThreadPool != null, "+++ task start error *TenetThreadPool is null object");
        /**
         * 先启动producer，在启动线程池，最后启动consumer
         */
        CountDownLatch countDownLatch = new CountDownLatch(tenetWorkerTaskContainer.getTaskContainer().size());
        taskContainer.forEach((k, v) -> {
            v.setStartLock(countDownLatch);
            executor.execute(v);
        });

        try {
            countDownLatch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.error("---- Tenent Worker Manager start timeout");
            stop();
            throw new Error("--- consumer,producer thread start error");
        }
        tenetThreadPool.prestartAllCoreThreads();
        workerMetricsGroup.recordTaskSuccess();
        log.info("++++++++ Tenet Worker Lifecycle started ++++++++");
        isRunning = true;
    }

    @Override
    public void stop() {
        log.info("-------- Tenet Worker stopping --------" );
        isRunning = false;
        /**first shutdown conusmer**/
        taskContainer.forEach((k, v) -> {
            if (k.connector().startsWith(TenetWorkerTaskConfigUtil.GENERATED_ID_WORKER_PREFIX)) {
                // consumer main thread stop
                stopTask(v.id());
                // clean batch message
                awaitStopTask(v.id(), Long.MAX_VALUE);
            }
        });

        tenetThreadPool.shutdown();
        while(!tenetThreadPool.isTerminated()){
            log.info("-------- Tenet Worker Thread Pool --------" );
        }
        /**shutdown producer**/
        taskContainer.forEach((k, v) -> {
            //updateStatus
            stopTask(v.id());
            awaitStopTask(v.id(), Long.MAX_VALUE);
        });
        if (!taskContainer.isEmpty()) {
            log.warn("Shutting down tasks uncleanly; herder should have shut down tasks before the Worker is stopped");
            executor.shutdown();
        }

        workerMetricsGroup.close();
        //workerConfigTransformer.close();
    }

    public void stopTask(TenetTaskId taskId) {
        try (LoggingContext loggingContext = LoggingContext.forTask(taskId)) {
            WorkerTask task = taskContainer.get(taskId);
            if (task == null) {
                log.warn("Ignoring stop request for unowned task {}", taskId);
                return;
            }

            log.info("Stopping task {}", task.id());

            task.stop();

        }
    }

    protected void awaitStopTask(TenetTaskId taskId, long timeout) {
        try (LoggingContext loggingContext = LoggingContext.forTask(taskId)) {
            WorkerTask task = taskContainer.remove(taskId);
            if (task == null) {
                log.warn("Ignoring await stop request for non-present task {}", taskId);
                return;
            }

            if (!task.awaitStop(timeout)) {
                log.error("Graceful stop of task {} failed.", task.id());
                task.cancel();
            } else {
                log.debug("Graceful stop of task {} succeeded.", task.id());
            }
        }
    }

    public boolean isRunning() {
        return isRunning;
    }

    public ConcurrentReferenceHashMap<TenetTaskId, WorkerTask> getTaskContainer() {
        return taskContainer;
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    /**
     * 小于守护线程 Phase，比守护线程先启动
     * @return
     */
    @Override
    public int getPhase() {
        return 0;
    }
}

