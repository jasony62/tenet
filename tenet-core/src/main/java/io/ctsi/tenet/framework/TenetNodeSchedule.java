package io.ctsi.tenet.framework;

import   io.ctsi.tenet.framework.config.TenetNodeScheduleConfig;
import   io.ctsi.tenet.framework.config.TenetNodeTaskConfig;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.PreDestroy;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Mc.D
 */
@Slf4j
public class TenetNodeSchedule {
    private static final float UP_LINE = 1.2f;
    private static final float DOWN_LINE = 0.8f;

    private final ScheduledThreadPoolExecutor daemonExecutor;
    private volatile ThreadPoolExecutor taskExecutor;
    private volatile ScheduledFuture<?> future;
    //单线程读
    private final ScheduledFuture<?> checkFuture;

    private final TenetNodeScheduleConfig config;

    private final TenetNodeTaskConfig taskConfig;

    private final   io.ctsi.tenet.framework.TenetNodeTaskHolder holder;

    private final AtomicInteger threadCount = new AtomicInteger(1);

    private final AtomicInteger fullTime = new AtomicInteger(0);
    private final AtomicInteger emptyTime = new AtomicInteger(0);

    private final AtomicLong taskTimer = new AtomicLong(0);
    private final AtomicLong threadTimer = new AtomicLong(0);

    @Getter
    private volatile boolean overloading = false;

    @Getter
    private volatile boolean emptyloading = false;
    /**
     * 当前频率（纳秒）
     */
    @Getter
    private volatile long currentRate;

    /**
     * 当前线程数
     */
    @Getter
    private volatile int currentThreadCount;

    private volatile boolean exit;

    public TenetNodeSchedule(TenetNodeScheduleConfig scheduleConfig, TenetNodeTaskConfig taskConfig,   io.ctsi.tenet.framework.TenetNodeTaskHolder holder) {
        this.config = scheduleConfig;
        this.taskConfig = taskConfig;
        this.holder = holder;

        taskExecutor = makeTaskPool();
        taskExecutor.prestartAllCoreThreads();

        daemonExecutor = new ScheduledThreadPoolExecutor(5, r -> {
            Thread t = new Thread(r);
            t.setName(taskConfig.getHolder() + "-pool-daemon-thread");
            t.setDaemon(true);
            t.setPriority(8);
            return t;
        });
        daemonExecutor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        daemonExecutor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        daemonExecutor.setRemoveOnCancelPolicy(true);
        log.info("init {} thread pool on {} core threads", taskConfig.getHolder(), config.getDefaultThread());
        start();
        //插入监控任务
        checkFuture = daemonExecutor.scheduleWithFixedDelay(checkThread(), 10, 1, TimeUnit.SECONDS);

    }


    /**
     * 以默认的频率开始任务
     */
    public void start() {
        if (currentRate == 0) {
            currentRate = TimeUnit.MILLISECONDS.toNanos(config.getDefaultRate());
        }
        start(currentRate);
    }

    /**
     * 以固定的频率开始任务
     *
     * @param defaultRate 默认频率（ns)
     */
    public void start(long defaultRate) {
        if (isStopping() || exit) {
            return;
        }
        if (future != null && !future.isDone()) {
            stop(true);
        }
        currentRate = defaultRate;
        future = daemonExecutor.scheduleAtFixedRate(() -> {
            long n1 = System.nanoTime();
            taskExecutor.submit(holder.getTask());
            log.debug("submit task in {}ns", System.nanoTime() - n1);
        }, 0, defaultRate, TimeUnit.NANOSECONDS);
        log.info("firing {} task on {}ns/time.", taskConfig.getHolder(), defaultRate);

    }

    /**
     * 停止任务（等待任务执行完成）
     */
    public void stop() {
        stop(false);
    }

    /**
     * 停止任务
     *
     * @param forceStop 是否强制终止
     */
    public void stop(boolean forceStop) {
        if (future.isDone()) {
            return;
        }
        future.cancel(forceStop);
        log.info("{}stop fire {} task...", forceStop ? "forced " : "", taskConfig.getHolder());
    }

    /**
     * 按指定间隔重启任务线程
     * <p>
     * 注意：如果任务执行时间超过300ms，有可能被强制中断，如果线程停止响应，此处可能触发死循环
     *
     * @param defaultRate 任务发射间隔（ns）
     */
    public void restart(long defaultRate) {
        if (future != null && !future.isDone()) {
            stop();
            // 300ms超时，等待任务完成，如果不能完成直接中断
            long ms = System.currentTimeMillis();
            while (!future.isDone()) {
                if (System.currentTimeMillis() - ms > 300) {
                    stop(true);
                }
            }
        }
        currentRate = defaultRate;
        start(defaultRate);
    }

    /**
     * 按默认频率降低任务速率，发送间隔为当前间隔*步长
     */
    public void slowDown() {
        changeRate(config.getDefaultSlowdownStep());
    }

    /**
     * 按默认频率提高任务速率，发送间隔为当前间隔*步长，最低间隔1ns
     */
    public void faster() {
        changeRate(config.getDefaultFasterStep());
    }

    /**
     * 调整任务发送频率，发送间隔为当前间隔*权重，最低间隔1ns
     *
     * @param weight 权重
     */
    public void changeRate(double weight) {
        //assert config.getStep() > 1 : "倍率需大于1";
        if (weight <= 0) {
            return;
        }
        if (currentRate == 0) {
            currentRate = TimeUnit.MILLISECONDS.toNanos(config.getDefaultRate());
        }
        long r = (long) (currentRate * weight);
        if (r < 1) {
            if (currentRate != 1) {
                r = 1;
            } else {
                log.warn("already fastest!");
                return;
            }
        }
        restart(r);
    }

    /**
     * 增加、减少线程池线程数量
     *
     * @param count 增加的数量
     */
    public void addTaskThread(int count) {
        if (currentThreadCount + count < 0) {
            count = -currentThreadCount + 1;
        }
        int t = currentThreadCount;
        t += count;
        if (count > 0) {
            taskExecutor.setMaximumPoolSize(t);
            taskExecutor.setCorePoolSize(t);
            taskExecutor.prestartAllCoreThreads();
        } else {
            taskExecutor.setCorePoolSize(t);
            taskExecutor.setMaximumPoolSize(t);
        }
        currentThreadCount = t;
    }

    /**
     * 监测线程任务，用于动态控制线程池任务线程生成频率。
     *
     * @return 任务体
     */
    private Runnable checkThread() {

        return () -> {
            if (exit) {
                return;
            }
            if (isStopping()) {
                log.warn("checked a stopping pool! try to restart it...");
                taskExecutor = makeTaskPool();
                taskExecutor.prestartAllCoreThreads();
                start();
            }
            long emptyCount = holder.getEmptyCount().getAndSet(0);
            long runCount = holder.getRunCount().getAndSet(0);
            int queueSize = taskExecutor.getQueue().size();
            log.info("checked {} empty tasks and {} normal tasks in this time", emptyCount, runCount);
            log.info("activeCount is {},queue size is {},threads count is {},fare rate: {}", taskExecutor.getActiveCount(), queueSize, currentThreadCount, currentRate);
            if (overloading) {
                log.warn("overloading now!");
            }
            if (!config.isActiveSchedule()) {
                return;
            }
            /*
            单周期需要处理两类事务：
            1、尽量保持队列中的任务数量在一倍任务数，上下浮动为20%
            2、空跑任务数维持在核心线程数附近且不超过正常任务数
             */
            long allTaskCount = runCount + emptyCount;
            if (queueSize > allTaskCount * UP_LINE) {
                log.info("too many tasks");
                long timeWeight = taskTimer.incrementAndGet();
                if (timeWeight <= 0) {
                    taskTimer.set(1);
                    timeWeight = 1;
                }
                if (!config.isAutoCalcStep()) {
                    this.slowDown();
                } else {
                    double delta = queueSize - allTaskCount;
                    double weight = delta / allTaskCount / timeWeight;
                    if (weight < 1) {
                        changeRate(1 + weight);
                    } else {
                        changeRate(weight);
                    }
                }
            } else if (queueSize < allTaskCount * DOWN_LINE) {
                log.info("need more tasks");
                long timeWeight = taskTimer.decrementAndGet();
                if (timeWeight >= 0) {
                    taskTimer.set(-1);
                    timeWeight = -1;
                }
                timeWeight = -timeWeight;
                if (!config.isAutoCalcStep()) {
                    this.faster();
                } else {
                    double delta = allTaskCount - queueSize;
                    double weight = (1 - delta / allTaskCount) * timeWeight;
                    if (weight == 0) {
                        changeRate(0.1);
                    } else {
                        changeRate(Math.min(weight, 0.9));
                    }
                }
            } else {
                taskTimer.set(0);
            }

            if (currentThreadCount >= config.getMaxThread()) {
                log.info("threads full");
                fullTime.incrementAndGet();
            } else {
                fullTime.set(0);
            }
            if (runCount == 0) {
                log.info("no tasks");
                emptyTime.incrementAndGet();
            } else {
                emptyTime.set(0);
            }

            /*
            结束过载的时机：
            1、空跑任务数大于正常任务数
             */
            if (emptyCount > runCount && currentThreadCount > config.getDefaultThread()) {
                log.info("too many threads");
                if (threadTimer.getAndIncrement() < 0) {
                    threadTimer.set(1);
                }
                if (overloading) {
                    log.info("exit overload...");
                    overloading = false;
                }
                //可减少的
                int canDes = currentThreadCount - config.getDefaultThread();
                if (emptyTime.get() > 0) {
                    this.addTaskThread(-canDes);
                } else {
                    //空任务越多，比值越大
                    double weight = (double) emptyCount / runCount - 1;
                    double des = weight * canDes * 0.5;
                    if (des < 1) {
                        des = 1;
                    } else if (des > canDes) {
                        des = canDes;
                    }
                    this.addTaskThread(-(int) des);
                }
            } else if (emptyCount < config.getDefaultThread() * DOWN_LINE) {
                log.info("need more thread...");
                if (threadTimer.getAndDecrement() > 0) {
                    threadTimer.set(-1);
                }
                if (canAddThread()) {
                    this.addTaskThread(1);
                } else if (!overloading && config.isAlowOverloading() && fullTime.get() > config.getFullTimeBeforeOverload()) {
                    log.warn("begin overloading now!");
                    overloading = true;
                    this.addTaskThread(1);
                }
            } else {
                threadTimer.set(0);
            }
            //TODO other check for control the thread pool
        };
    }

    private boolean canAddThread() {
        return currentThreadCount < config.getMaxThread() || (overloading && currentThreadCount < config.getMaxThread() * config.getOverloadPercent() / 100);
    }

    private ThreadPoolExecutor makeTaskPool() {
        /*
            线程池的外部控制方法：
            1、守护线程动态调整线程池线程数
            2、将核心线程和线程池最大线程设置为同一个值，这样线程池将只存在固定数量的线程
            3、调整线程数需要同时调整核心线程及最大线程，增加时先增加最大线程，再增加核心线程，减少时先减少核心线程，再减少最大线程
         */
        this.currentRate = TimeUnit.MILLISECONDS.toNanos(config.getDefaultRate());
        this.currentThreadCount = config.getDefaultThread();
        threadCount.set(1);

        return new ThreadPoolExecutor(config.getDefaultThread(),
                config.isActiveSchedule() ? config.getDefaultThread() : config.getMaxThread(),
                10, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(config.isActiveSchedule() ? Integer.MAX_VALUE : config.getMaxThread()), r -> {
            Thread t = new Thread(r);
            t.setName(taskConfig.getHolder() + "-pool-thread-" + threadCount.getAndIncrement());
            t.setDaemon(false);
            t.setPriority(Thread.NORM_PRIORITY);
            return t;
        });
    }


    @PreDestroy
    public void destory() {
        exit = true;
        stop();
        taskExecutor.shutdown();
        checkFuture.cancel(true);
        daemonExecutor.shutdown();
        try {
            if (!taskExecutor.awaitTermination(300, TimeUnit.MILLISECONDS)) {
                taskExecutor.shutdownNow();
            }
        } catch (Exception e) {
            log.error("", e);
        }
    }

    private boolean isStopping() {
        return taskExecutor.isShutdown() || taskExecutor.isTerminated() || taskExecutor.isTerminating();
    }

    public ThreadPoolExecutor getTaskExecutor() {
        return taskExecutor;
    }
}
