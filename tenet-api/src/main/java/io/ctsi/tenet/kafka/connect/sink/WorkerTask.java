package io.ctsi.tenet.kafka.connect.sink;

import io.ctsi.tenet.kafka.connect.AbstractStatus.State;
import io.ctsi.tenet.kafka.connect.TenetTaskId;
import io.ctsi.tenet.kafka.connect.TopicStatus;
import io.ctsi.tenet.kafka.connect.metrics.ConnectMetrics;
import io.ctsi.tenet.kafka.connect.metrics.ConnectMetricsRegistry;
import io.ctsi.tenet.kafka.connect.policy.RetryWithToleranceOperator;
import io.ctsi.tenet.kafka.connect.storage.StatusBackingStore;
import io.ctsi.tenet.kafka.connect.util.LoggingContext;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Frequencies;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public abstract class WorkerTask implements Runnable{
    private static final Logger log = LoggerFactory.getLogger(WorkerTask.class);
    private static final String THREAD_NAME_PREFIX = "task-thread-";

    protected final TenetTaskId id;
    private final TaskStatus.Listener statusListener;
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);
    private final TaskMetricsGroup taskMetricsGroup;
    private volatile TargetState targetState;
    private volatile boolean stopping;   // indicates whether the Worker has asked the task to stop
    private volatile boolean cancelled;  // indicates whether the Worker has cancelled the task (e.g. because of slow shutdown)
    protected final StatusBackingStore statusBackingStore;
    protected final RetryWithToleranceOperator retryWithToleranceOperator;
    protected final Time time;
    private CountDownLatch startLatch = new CountDownLatch(1);

    public WorkerTask(TenetTaskId id,
                      TaskStatus.Listener statusListener,
                      TargetState initialState,
                      ConnectMetrics connectMetrics,
                      RetryWithToleranceOperator retryWithToleranceOperator,
                      Time time,
                      StatusBackingStore statusBackingStore) {
        this.id = id;
        this.taskMetricsGroup = new TaskMetricsGroup(this.id, connectMetrics, statusListener);
        this.statusListener = statusListener;
        this.targetState = initialState;
        this.stopping = false;
        this.cancelled = false;
        this.taskMetricsGroup.recordState(this.targetState);
        this.retryWithToleranceOperator = retryWithToleranceOperator;
        this.statusBackingStore = statusBackingStore;
        this.time = time;
    }

    public TenetTaskId id() {
        return id;
    }

    /**
     * Initialize the task for execution.
     *
     * @param taskConfig initial configuration
     */
    public abstract void initialize(TaskConfig taskConfig);


    private void triggerStop() {
        log.debug("--triggerStop");
        synchronized (this) {
            stopping = true;

            // wakeup any threads that are waiting for unpause
            this.notifyAll();
        }
    }

    /**
     * Stop this task from processing messages. This method does not block, it only triggers
     * shutdown. Use #{@link #awaitStop} to block until completion.
     */
    public void stop() {
        triggerStop();
    }

    /**
     * Cancel this task. This won't actually stop it, but it will prevent the state from being
     * updated when it eventually does shutdown.
     */
    public void cancel() {
        cancelled = true;
    }

    /**
     * Wait for this task to finish stopping.
     *
     * @param timeoutMs time in milliseconds to await stop
     * @return true if successful, false if the timeout was reached
     */
    public boolean awaitStop(long timeoutMs) {
        try {
            return shutdownLatch.await(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return false;
        }
    }

    protected abstract void execute();

    protected abstract void close();

    /**
     * Method called when this worker task has been completely closed, and when the subclass should clean up
     * all resources.
     */
    //protected abstract void releaseResources();

    protected boolean isStopping() {
        return stopping;
    }

    /**
     * 模版方法
     */
    private void doClose() {
        try {
            close();
        } catch (Throwable t) {
            log.error("{} Task threw an uncaught and unrecoverable exception during shutdown", this, t);
            throw t;
        }
    }

    private void doRun() throws InterruptedException {
        try {
            synchronized (this) {
                if (stopping)
                    return;

                if (targetState == TargetState.PAUSED) {
                    onPause();
                    if (!awaitUnpause()) return;
                }

                statusListener.onStartup(id);
            }

            execute();
        } catch (Throwable t) {
            log.error("{} Task threw an uncaught and unrecoverable exception", this, t);
            log.error("{} Task is being killed and will not recover until manually restarted", this);
            throw t;
        } finally {
            /**当执行stop
             * 线程跳出循环调用doClose,doClose->close
             * **/
            log.debug("--close task maybe consumer or producer");
            doClose();
        }
    }

    private void onShutdown() {
        synchronized (this) {
            log.debug("--doShutdown {}",Thread.currentThread().getName());
            triggerStop();
            // if we were cancelled, skip the status update since the task may have already been
            // started somewhere else
            if (!cancelled)
                statusListener.onShutdown(id);
        }
    }

    protected void onFailure(Throwable t) {
        synchronized (this) {
            triggerStop();

            // if we were cancelled, skip the status update since the task may have already been
            // started somewhere else
            if (!cancelled)
                statusListener.onFailure(id, t);
        }
    }

    protected synchronized void onPause() {
        statusListener.onPause(id);
    }

    protected synchronized void onResume() {
        statusListener.onResume(id);
    }

    @Override
    public void run() {
        // TenetTaskManager启动set
        if(Objects.isNull(startLatch)) {
            log.error("--- Tenet Task run failed id is {}",id().task());
            throw new Error();
        }
        startLatch.countDown();
        // Clear all MDC parameters, in case this thread is being reused
        LoggingContext.clear();

        try (LoggingContext loggingContext = LoggingContext.forTask(id())) {
           // ClassLoader savedLoader = Plugins.compareAndSwapLoaders(loader);
            String savedName = Thread.currentThread().getName();
            try {
                Thread.currentThread().setName(THREAD_NAME_PREFIX + id);
                doRun();
                onShutdown();
            } catch (Throwable t) {
                onFailure(t);

                if (t instanceof Error)
                    throw (Error) t;
            } finally {
                try {
                    Thread.currentThread().setName(savedName);
                    //Plugins.compareAndSwapLoaders(savedLoader);
                    shutdownLatch.countDown();
                } finally {
                    try {
                        //releaseResources();
                    } finally {
                        taskMetricsGroup.close();
                    }
                }
            }
        }
    }

    public boolean shouldPause() {
        return this.targetState == TargetState.PAUSED;
    }

    /**
     * Await task resumption.
     *
     * @return true if the task's target state is not paused, false if the task is shutdown before resumption
     * @throws InterruptedException
     */
    protected boolean awaitUnpause() throws InterruptedException {
        synchronized (this) {
            while (targetState == TargetState.PAUSED) {
                if (stopping)
                    return false;
                this.wait();
            }
            return true;
        }
    }

    public void transitionTo(TargetState state) {
        synchronized (this) {
            // ignore the state change if we are stopping
            if (stopping)
                return;

            this.targetState = state;
            //this.notifyAll();
        }
    }

    protected void recordActiveTopic(String topic) {
        if (statusBackingStore.getTopic(id.connector(), topic) != null) {
            // The topic is already recorded as active. No further action is required.
            return;
        }
        statusBackingStore.put(new TopicStatus(topic, id, time.milliseconds()));
    }

    /**
     * Record that offsets have been committed.
     *
     * @param duration the length of time in milliseconds for the commit attempt to complete
     * @param error the unexpected error that occurred; may be null in the case of timeouts or interruptions
     */
    protected void recordCommitFailure(long duration, Throwable error) {
        taskMetricsGroup.recordCommit(duration, false, error);
    }

    /**
     * Record that offsets have been committed.
     *
     * @param duration the length of time in milliseconds for the commit attempt to complete
     */
    protected void recordCommitSuccess(long duration) {
        taskMetricsGroup.recordCommit(duration, true, null);
    }

    /**
     * Record that a batch of records has been processed.
     *
     * @param size the number of records in the batch
     */
    protected void recordBatch(int size) {
        taskMetricsGroup.recordBatch(size);
    }

    public void setStartLock(CountDownLatch startLock) {
        this.startLatch = startLock;
    }

    static class TaskMetricsGroup implements TaskStatus.Listener {
        private final TaskStatus.Listener delegateListener;
        private final StateTracker taskStateTimer;
        private final Time time;
        private final ConnectMetrics.MetricGroup metricGroup;
        private final Sensor commitTime;
        private final Sensor batchSize;
        private final Sensor commitAttempts;

        public TaskMetricsGroup(TenetTaskId id,  ConnectMetrics connectMetrics, TaskStatus.Listener statusListener) {
//            delegateListener = statusListener;
//            taskStateTimer = new StateTracker();
//            time =  Time.SYSTEM;

            delegateListener = statusListener;
            time = connectMetrics.time();
            taskStateTimer = new StateTracker();
            ConnectMetricsRegistry registry = connectMetrics.registry();
            metricGroup = connectMetrics.group(registry.taskGroupName(),
                    registry.connectorTagName(), id.connector(),
                    registry.taskTagName(), Integer.toString(id.task()));
            // prevent collisions by removing any previously created metrics in this group.
            metricGroup.close();

            metricGroup.addValueMetric(registry.taskStatus, new ConnectMetrics.LiteralSupplier<String>() {
                @Override
                public String metricValue(long now) {
                    return taskStateTimer.currentState().toString().toLowerCase(Locale.getDefault());
                }
            });

            addRatioMetric(State.RUNNING, registry.taskRunningRatio);
            addRatioMetric(State.PAUSED, registry.taskPauseRatio);

            commitTime = metricGroup.sensor("commit-time");
            commitTime.add(metricGroup.metricName(registry.taskCommitTimeMax), new Max());
            commitTime.add(metricGroup.metricName(registry.taskCommitTimeAvg), new Avg());

            batchSize = metricGroup.sensor("batch-size");
            batchSize.add(metricGroup.metricName(registry.taskBatchSizeMax), new Max());
            batchSize.add(metricGroup.metricName(registry.taskBatchSizeAvg), new Avg());

            MetricName offsetCommitFailures = metricGroup.metricName(registry.taskCommitFailurePercentage);
            MetricName offsetCommitSucceeds = metricGroup.metricName(registry.taskCommitSuccessPercentage);
            Frequencies commitFrequencies = Frequencies.forBooleanValues(offsetCommitFailures, offsetCommitSucceeds);
            commitAttempts = metricGroup.sensor("offset-commit-completion");
            commitAttempts.add(commitFrequencies);
        }

        @Override
        public void onStartup(TenetTaskId id) {
            taskStateTimer.changeState(State.RUNNING, time.milliseconds());
            delegateListener.onStartup(id);
        }

        @Override
        public void onFailure(TenetTaskId id, Throwable cause) {
            taskStateTimer.changeState(State.FAILED, time.milliseconds());
            delegateListener.onFailure(id, cause);
        }

        @Override
        public void onPause(TenetTaskId id) {
            taskStateTimer.changeState(State.PAUSED, time.milliseconds());
            delegateListener.onPause(id);
        }

        @Override
        public void onResume(TenetTaskId id) {
            taskStateTimer.changeState(State.RUNNING, time.milliseconds());
            delegateListener.onResume(id);
        }

        @Override
        public void onShutdown(TenetTaskId id) {
            taskStateTimer.changeState(State.UNASSIGNED, time.milliseconds());
            delegateListener.onShutdown(id);
        }

        @Override
        public void onDeletion(TenetTaskId id) {
            taskStateTimer.changeState(State.DESTROYED, time.milliseconds());
            delegateListener.onDeletion(id);
        }

        public void recordState(TargetState state) {
            switch (state) {
                case STARTED:
                    taskStateTimer.changeState(State.RUNNING, time.milliseconds());
                    break;
                case PAUSED:
                    taskStateTimer.changeState(State.PAUSED, time.milliseconds());
                    break;
                default:
                    break;
            }
        }

        public State state() {
            return taskStateTimer.currentState();
        }

        private void addRatioMetric(final State matchingState, MetricNameTemplate template) {
            MetricName metricName = metricGroup.metricName(template);
            if (metricGroup.metrics().metric(metricName) == null) {
                metricGroup.metrics().addMetric(metricName, new Measurable() {
                    @Override
                    public double measure(MetricConfig config, long now) {
                        return taskStateTimer.durationRatio(matchingState, now);
                    }
                });
            }
        }

        void close() {
            metricGroup.close();
        }

        void recordCommit(long duration, boolean success, Throwable error) {
            if (success) {
                commitTime.record(duration);
                commitAttempts.record(1.0d);
            } else {
                commitAttempts.record(0.0d);
            }
        }

        void recordBatch(int size) {
            batchSize.record(size);
        }
    }

    /**
     * Remove all metrics published by this task.
     */
    public void removeMetrics() {
        taskMetricsGroup.close();
    }

    public TenetTaskId getId() {
        return id;
    }
}