package io.ctsi.tenet.kafka.connect.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SourceTaskOffsetCommitter {
    private static final Logger log = LoggerFactory.getLogger(SourceTaskOffsetCommitter.class);

//    private final WorkerConfig config;
//    private final ScheduledExecutorService commitExecutorService;
//    private final ConcurrentMap<TenetTaskId, ScheduledFuture<?>> committers;
//
//    // visible for testing
//    SourceTaskOffsetCommitter(WorkerConfig config,
//                              ScheduledExecutorService commitExecutorService,
//                              ConcurrentMap<TenetTaskId, ScheduledFuture<?>> committers) {
//        this.config = config;
//        this.commitExecutorService = commitExecutorService;
//        this.committers = committers;
//    }
//
//    public SourceTaskOffsetCommitter(WorkerConfig config) {
//        this(config, Executors.newSingleThreadScheduledExecutor(ThreadUtils.createThreadFactory(
//                SourceTaskOffsetCommitter.class.getSimpleName() + "-%d", false)),
//                new ConcurrentHashMap<TenetTaskId, ScheduledFuture<?>>());
//    }
//
//    public void close(long timeoutMs) {
//        commitExecutorService.shutdown();
//        try {
//            if (!commitExecutorService.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS)) {
//                log.error("Graceful shutdown of offset commitOffsets thread timed out.");
//            }
//        } catch (InterruptedException e) {
//            // ignore and allow to exit immediately
//        }
//    }
//
//    public void schedule(final TenetTaskId id, final WorkerSourceTask workerTask) {
//        long commitIntervalMs = config.getLong(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG);
//        ScheduledFuture<?> commitFuture = commitExecutorService.scheduleWithFixedDelay(new Runnable() {
//            @Override
//            public void run() {
//                try (LoggingContext loggingContext = LoggingContext.forOffsets(id)) {
//                    commit(workerTask);
//                }
//            }
//        }, commitIntervalMs, commitIntervalMs, TimeUnit.MILLISECONDS);
//        committers.put(id, commitFuture);
//    }
//
//    public void remove(TenetTaskId id) {
//        final ScheduledFuture<?> task = committers.remove(id);
//        if (task == null)
//            return;
//
//        try (LoggingContext loggingContext = LoggingContext.forTask(id)) {
//            task.cancel(false);
//            if (!task.isDone())
//                task.get();
//        } catch (CancellationException e) {
//            // ignore
//            log.trace("Offset commit thread was cancelled by another thread while removing connector task with id: {}", id);
//        } catch (ExecutionException | InterruptedException e) {
//            throw new ConnectException("Unexpected interruption in SourceTaskOffsetCommitter while removing task with id: " + id, e);
//        }
//    }
//
//    private void commit(WorkerSourceTask workerTask) {
//        log.debug("{} Committing offsets", workerTask);
//        try {
//            if (workerTask.commitOffsets()) { ///****jdbc commit Offset******/
//                return;
//            }
//            log.error("{} Failed to commit offsets", workerTask);
//        } catch (Throwable t) {
//            // We're very careful about exceptions here since any uncaught exceptions in the commit
//            // thread would cause the fixed interval schedule on the ExecutorService to stop running
//            // for that task
//            log.error("{} Unhandled exception when committing: ", workerTask, t);
//        }
//    }
}
