package io.ctsi.tenet.kafka.connect.sink;

import io.ctsi.tenet.kafka.connect.error.RetriableException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

public abstract  class SinkTask {
    private static final Logger log = LoggerFactory.getLogger(SinkTask.class);
    /**
     * <p>
     * The configuration key that provides the list of topics that are inputs for this
     * SinkTask.
     * </p>
     */
    public static final String TOPICS_CONFIG = "topics";

    /**
     * <p>
     * The configuration key that provides a regex specifying which topics to include as inputs
     * for this SinkTask.
     * </p>
     */
    public static final String TOPICS_REGEX_CONFIG = "topics.regex";

    protected SinkTaskContext context;

    /**
     * Initialize the context of this task. Note that the partition assignment will be empty until
     * Connect has opened the partitions for writing with {@link #open(Collection)}.
     * @param context The sink task's context
     */
    public void initialize(SinkTaskContext context) {
        this.context = context;
    }

    /**
     * Start the Task. This should handle any configuration parsing and one-time setup of the task.
     * @param props initial configuration
     */
    public abstract void start(Map<String, String> props);

    /**
     * Put the records in the sink. Usually this should send the records to the sink asynchronously
     * and immediately return.
     *
     * If this operation fails, the SinkTask may throw a {@link RetriableException} to
     * indicate that the framework should attempt to retry the same call again. Other exceptions will cause the task to
     * be stopped immediately. {@link SinkTaskContext#timeout(long)} can be used to set the maximum time before the
     * batch will be retried.
     *
     * @param records the set of records to send
     */
    public abstract void put(Collection<io.ctsi.tenet.kafka.connect.sink.SinkRecord> records);

    /**
     * Flush all records that have been {@link #put(Collection)} for the specified topic-partitions.
     *
     * @param currentOffsets the current offset state as of the last call to {@link #put(Collection)}},
     *                       provided for convenience but could also be determined by tracking all offsets included in the {@link io.ctsi.tenet.kafka.connect.sink.SinkRecord}s
     *                       passed to {@link #put}.
     */
    public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    }

    /**
     * Pre-commit hook invoked prior to an offset commit.
     *
     * The default implementation simply invokes {@link #flush(Map)} and is thus able to assume all {@code currentOffsets} are safe to commit.
     *
     * @param currentOffsets the current offset state as of the last call to {@link #put(Collection)}},
     *                       provided for convenience but could also be determined by tracking all offsets included in the {@link io.ctsi.tenet.kafka.connect.sink.SinkRecord}s
     *                       passed to {@link #put}.
     *
     * @return an empty map if Connect-managed offset commit is not desired, otherwise a map of offsets by topic-partition that are safe to commit.
     */
    public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        flush(currentOffsets);
        return currentOffsets;
    }

    /**
     * The SinkTask use this method to create writers for newly assigned partitions in case of partition
     * rebalance. This method will be called after partition re-assignment completes and before the SinkTask starts
     * fetching data. Note that any errors raised from this method will cause the task to stop.
     * @param partitions The list of partitions that are now assigned to the task (may include
     *                   partitions previously assigned to the task)
     */
    public void open(Collection<TopicPartition> partitions) {
        this.onPartitionsAssigned(partitions);
    }

    /**
     * @deprecated Use {@link #open(Collection)} for partition initialization.
     */
    @Deprecated
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    }

    /**
     * The SinkTask use this method to close writers for partitions that are no
     * longer assigned to the SinkTask. This method will be called before a rebalance operation starts
     * and after the SinkTask stops fetching data. After being closed, Connect will not write
     * any records to the task until a new set of partitions has been opened. Note that any errors raised
     * from this method will cause the task to stop.
     * @param partitions The list of partitions that should be closed
     */
    public void close(Collection<TopicPartition> partitions) {
        this.onPartitionsRevoked(partitions);
    }

    /**
     * @deprecated Use {@link #close(Collection)} instead for partition cleanup.
     */
    @Deprecated
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    }

    /**
     * Perform any cleanup to stop this task. In SinkTasks, this method is invoked only once outstanding calls to other
     * methods have completed (e.g., {@link #put(Collection)} has returned) and a final {@link #flush(Map)} and offset
     * commit has completed. Implementations of this method should only need to perform final cleanup operations, such
     * as closing network connections to the sink system.
     */

    public abstract void stop();

}
