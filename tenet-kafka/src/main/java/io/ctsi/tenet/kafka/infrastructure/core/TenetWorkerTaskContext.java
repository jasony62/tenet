package io.ctsi.tenet.kafka.infrastructure.core;

import io.ctsi.tenet.kafka.connect.error.IllegalWorkerStateException;
import io.ctsi.tenet.kafka.connect.sink.SinkTaskContext;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class TenetWorkerTaskContext implements SinkTaskContext {

    private final Logger log = LoggerFactory.getLogger(getClass());
    private Map<TopicPartition, Long> offsets;
    private long timeoutMs;
    private KafkaConsumer<byte[], byte[]> consumer;
    private final TenetWorkerTask sinkTask;

    private final Set<TopicPartition> pausedPartitions;
    private boolean commitRequested;

    public TenetWorkerTaskContext(KafkaConsumer<byte[], byte[]> consumer,
                                 TenetWorkerTask sinkTask) {
        this.offsets = new HashMap<>();
        this.timeoutMs = -1L;
        this.consumer = consumer;
        this.sinkTask = sinkTask;
        this.pausedPartitions = new HashSet<>();
    }

    @Override
    public Map<String, String> configs() {
        return null;
    }

    @Override
    public void offset(Map<TopicPartition, Long> offsets) {
        log.debug("{} Setting offsets for topic partitions {}", this, offsets);
        this.offsets.putAll(offsets);
    }

    @Override
    public void offset(TopicPartition tp, long offset) {
        log.debug("{} Setting offset for topic partition {} to {}", this, tp, offset);
        offsets.put(tp, offset);
    }

    public void clearOffsets() {
        offsets.clear();
    }

    /**
     * Get offsets that the SinkTask has submitted to be reset. Used by the Kafka Connect framework.
     * @return the map of offsets
     */
    public Map<TopicPartition, Long> offsets() {
        return offsets;
    }

    @Override
    public void timeout(long timeoutMs) {
        log.debug("{} Setting timeout to {} ms", this, timeoutMs);
        this.timeoutMs = timeoutMs;
    }

    /**
     * Get the timeout in milliseconds set by SinkTasks. Used by the Kafka Connect framework.
     * @return the backoff timeout in milliseconds.
     */
    public long timeout() {
        return timeoutMs;
    }

    @Override
    public Set<TopicPartition> assignment() {
        if (consumer == null) {
            throw new IllegalWorkerStateException("SinkTaskContext may not be used to look up partition assignment until the task is initialized");
        }
        return consumer.assignment();
    }

    @Override
    public void pause(TopicPartition... partitions) {
        if (consumer == null) {
            throw new IllegalWorkerStateException("SinkTaskContext may not be used to pause consumption until the task is initialized");
        }
        try {
            Collections.addAll(pausedPartitions, partitions);
            if (sinkTask.shouldPause()) {
                log.debug("{} Connector is paused, so not pausing consumer's partitions {}", this, partitions);
            } else {
                consumer.pause(Arrays.asList(partitions));
                log.debug("{} Pausing partitions {}. Connector is not paused.", this, partitions);
            }
        } catch (IllegalStateException e) {
            throw new IllegalWorkerStateException("SinkTasks may not pause partitions that are not currently assigned to them.", e);
        }
    }

    @Override
    public void resume(TopicPartition... partitions) {
        if (consumer == null) {
            throw new IllegalWorkerStateException("SinkTaskContext may not be used to resume consumption until the task is initialized");
        }
        try {
            pausedPartitions.removeAll(Arrays.asList(partitions));
            if (sinkTask.shouldPause()) {
                log.debug("{} Connector is paused, so not resuming consumer's partitions {}", this, partitions);
            } else {
                consumer.resume(Arrays.asList(partitions));
                log.debug("{} Resuming partitions: {}", this, partitions);
            }
        } catch (IllegalStateException e) {
            throw new IllegalWorkerStateException("SinkTasks may not resume partitions that are not currently assigned to them.", e);
        }
    }

    public Set<TopicPartition> pausedPartitions() {
        return pausedPartitions;
    }

    @Override
    public void requestCommit() {
        log.debug("{} Requesting commit", this);
        commitRequested = true;
    }

    public boolean isCommitRequested() {
        return commitRequested;
    }

    public void clearCommitRequest() {
        commitRequested = false;
    }

    @Override
    public String toString() {
        return "WorkerSinkTaskContext{" +
                "id=" + sinkTask.getId() +
                '}';
    }
}
