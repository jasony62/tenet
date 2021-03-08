package io.ctsi.tenet.kafka.infrastructure.core;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class TenetProducerTaskContext{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private Map<TopicPartition, Long> offsets;
    private long timeoutMs;
    private final TenetProducerTask tenetProducerTask;

    private boolean commitRequested;

    public TenetProducerTaskContext(TenetProducerTask tenetProducerTask) {
       this.tenetProducerTask = tenetProducerTask;
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

    /**
     * Get the timeout in milliseconds set by SinkTasks. Used by the Kafka Connect framework.
     * @return the backoff timeout in milliseconds.
     */
    public long timeout() {
        return timeoutMs;
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
                "id=" + tenetProducerTask.id() +
                '}';
    }

}
