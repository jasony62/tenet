package io.ctsi.tenet.kafka.infrastructure.core;

import io.ctsi.tenet.kafka.config.TenetKafkaProperties;
import io.ctsi.tenet.kafka.connect.ConnectHeaders;
import io.ctsi.tenet.kafka.connect.Headers;
import io.ctsi.tenet.kafka.connect.TenetTaskId;
import io.ctsi.tenet.kafka.connect.error.ConnectException;
import io.ctsi.tenet.kafka.connect.error.RetriableException;
import io.ctsi.tenet.kafka.connect.metrics.ConnectMetrics;
import io.ctsi.tenet.kafka.connect.metrics.ConnectMetricsRegistry;
import io.ctsi.tenet.kafka.connect.policy.CumulativeSum;
import io.ctsi.tenet.kafka.connect.policy.RetryWithToleranceOperator;
import io.ctsi.tenet.kafka.connect.policy.Stage;
import io.ctsi.tenet.kafka.connect.storage.MemoryStatusBackingStore;
import io.ctsi.tenet.kafka.connect.transforms.TransformationChain;
import io.ctsi.tenet.kafka.connect.data.Converter;
import io.ctsi.tenet.kafka.connect.data.HeaderConverter;
import io.ctsi.tenet.kafka.connect.data.SchemaAndValue;
import io.ctsi.tenet.kafka.connect.data.error.DataException;
import io.ctsi.tenet.kafka.connect.sink.*;
import io.ctsi.tenet.kafka.task.QueueManager;
import io.ctsi.tenet.kafka.connect.sink.WorkerTask;
import io.ctsi.tenet.kafka.mongodb.util.ConnectUtils;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.Value;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.regex.Pattern;

import static java.util.Collections.singleton;

public class TenetWorkerTask extends WorkerTask {

    private static final Logger logger = LoggerFactory.getLogger(TenetWorkerTask.class);
    private final Map<TopicPartition, OffsetAndMetadata> origOffsets;
    protected final RetryWithToleranceOperator retryWithToleranceOperator;
    private final List<InternalSinkRecord> messageBatch;
    private final Converter keyConverter;
    private final Converter valueConverter;
    private final HeaderConverter headerConverter;
    private final SinkTaskMetricsGroup sinkTaskMetricsGroup;
    private final SinkTask sinkTask;
    private TenetWorkerTaskContext context;
    private final QueueManager queueManager;
    private Map<String, String> taskConfig;
    private final TransformationChain<SinkRecord> transformationChain;
    private boolean pausedForRedelivery;
    private final KafkaConsumer<byte[], byte[]> consumer;
    private Map<TopicPartition, OffsetAndMetadata> lastCommittedOffsets;
    private Map<TopicPartition, OffsetAndMetadata> currentOffsets;
    private RuntimeException rebalanceException;
    private int commitSeqno;
    private long nextCommit;
    private long commitStarted;
    private int commitFailures;
    private boolean committing;
    private final Time time;
    private final Long offSetFlushInterval;
    private boolean notPausedForDispatcher;

    private boolean taskStopped;

    public TenetWorkerTask(TenetTaskId id,
                           SinkTask sinkTask,
                           TaskStatus.Listener statusListener,
                           TargetState initialState,
                           Converter keyConverter,
                           Converter valueConverter,
                           HeaderConverter headerConverter,
                           TransformationChain<SinkRecord> transformationChain,
                           KafkaConsumer<byte[], byte[]> consumer,
                           Time time,
                           ConnectMetrics connectMetrics,
                           RetryWithToleranceOperator retryWithToleranceOperator,
                           MemoryStatusBackingStore memoryStatusBackingStore,
                           Long offSetFlushInterval,
                           QueueManager queueManager){

        super(id, statusListener ,initialState, connectMetrics,  retryWithToleranceOperator,time,memoryStatusBackingStore);

        this.origOffsets = new HashMap<>();
        this.retryWithToleranceOperator = retryWithToleranceOperator;
        this.messageBatch = new ArrayList<>();
        this.keyConverter = keyConverter;
        this.valueConverter = valueConverter;
        this.headerConverter = headerConverter;
        this.sinkTask = sinkTask;
        this.transformationChain = transformationChain;
        this.pausedForRedelivery = false;
        this.notPausedForDispatcher = false;
        this.currentOffsets = new HashMap<>();
        this.committing = false;
        this.commitSeqno = 0;
        this.commitStarted = -1;
        this.commitFailures = 0;
        this.offSetFlushInterval = offSetFlushInterval;
        this.nextCommit = time.milliseconds() +offSetFlushInterval;
        this.sinkTaskMetricsGroup = new SinkTaskMetricsGroup(id, connectMetrics);
        this.sinkTaskMetricsGroup.recordOffsetSequenceNumber(commitSeqno);
        this.time = time;
        this.consumer = consumer;
        this.queueManager = queueManager;
//        this.reentrantLock = new ReentrantLock();
//        this.waitLock = reentrantLock.newCondition();
        this.taskStopped = false;
    }
    /**spring**/
    public void initialize(TaskConfig taskConfig) {
        try {

            this.taskConfig = taskConfig.originalsStrings();
            this.context = new TenetWorkerTaskContext(this.consumer,this);
            logger.info("{} Sink task finished initialization and start", this);
            int max = Integer.valueOf(this.taskConfig.get("tenet.max.queue"));
            //this.queueManager.init(max,this.reentrantLock,this.waitLock);
            this.queueManager.init(max);

        } catch (Throwable t) {
            logger.error("{} Task failed initialization and will not be started.", this, t);
            onFailure(t);
        }
    }

    /**
     * Initializes and starts the SinkTask.
     */
    public void initializeAndStart() {
        TenetKafkaProperties.validate(taskConfig);

        if (TenetKafkaProperties.hasTopicsConfig(taskConfig)) {
            String[] topics = taskConfig.get(SinkTask.TOPICS_CONFIG).split(",");
            Arrays.setAll(topics, i -> topics[i].trim());
            consumer.subscribe(Arrays.asList(topics), new HandleRebalance());
            logger.debug("{} Initializing and starting task for topics {}", this, topics);
        } else {
            String topicsRegexStr = taskConfig.get(SinkTask.TOPICS_REGEX_CONFIG);
            Pattern pattern = Pattern.compile(topicsRegexStr);
            consumer.subscribe(pattern, new HandleRebalance());
            logger.debug("{} Initializing and starting task for topics regex {}", this, topicsRegexStr);
        }
        sinkTask.initialize(context);
        if(taskConfig.get("dialectName").startsWith("Mongo")) {
            taskConfig.put("topic_suffix","_in");
        }
        sinkTask.start(taskConfig);
        logger.info("{} Sink task finished initialization and start", this);
    }
    @Override
    public void stop() {
        logger.info("--consumer stop {}",this.messageBatch.isEmpty());
        // Offset commit is handled upon exit in work thread
        super.stop();
        consumer.wakeup();
    }
    /**
     *  shutdown invoke this method
     * **/
    @Override
    protected void close() {
        logger.info("--close sinkWorkTask");
        // FIXME Kafka needs to add a timeout parameter here for us to properly obey the timeout
        // passed in
        Utils.closeQuietly(consumer, "consumer");
        if (consumer != null) {
            try {
                consumer.close();
            } catch (Throwable t) {
                logger.warn("Could not close consumer", t);
            }
        }
        logger.info("--- close sinkWorkTask message size is {}",this.messageBatch.size());
        try {
            do {
                Thread.currentThread().sleep(1000);
                logger.info("--- close sinkWorkTask wait message size is {}",this.messageBatch.size());
            } while (!queueManager.addMessage(this.messageBatch));
        }catch(Exception e){
            logger.error("shutdown error is :",e);
        }
        this.messageBatch.clear();

        try {
            do {
                Thread.currentThread().sleep(1000);
                logger.info("--- close sinkWorkTask wait queueManager ctrl is {}",queueManager.getCtrol());
            } while (queueManager.getCtrol() != 0);
        }catch(Exception e){
            logger.error("shutdown error is :",e);
        }

        try {
            sinkTask.stop();
        } catch (Throwable t) {
            logger.warn("Could not stop task", t);
        }
        taskStopped = true;
        try {
            transformationChain.close();
        } catch (Throwable t) {
            logger.warn("Could not close transformation chain", t);
        }
        Utils.closeQuietly(retryWithToleranceOperator, "retry operator");
    }

    @Override
    public void transitionTo(TargetState state) {
        super.transitionTo(state);
        consumer.wakeup();
    }

    @Override
    public void execute() {
        initializeAndStart();
        // Make sure any uncommitted data has been committed and the task has
        // a chance to clean up its state
        logger.info("--- consumer start to poll kafka");
        try (UncheckedCloseable suppressible = this::closePartitions) {
            while (!isStopping())
                iteration();
        }
    }

    protected void iteration() {
        logger.trace("--- Consumer poll start shouldPause is {}", shouldPause());
        final long offsetCommitIntervalMs = offSetFlushInterval;
        logger.trace("--- Consumer pausedForDispatcher is {}", notPausedForDispatcher);

        try {

            long now = time.milliseconds();
            // Maybe commit
            logger.trace("--- Commit of offsets committing is {},boolean is {}", committing,now >= nextCommit);
            if (!committing && (context.isCommitRequested() || now >= nextCommit)) {
                commitOffsets(now, false);
                nextCommit = now + offsetCommitIntervalMs;
                context.clearCommitRequest();
            }
            logger.trace("--- Commit of offsets committing is {},next time is {}", committing,nextCommit);
            final long commitTimeoutMs = commitStarted + offSetFlushInterval;
            logger.trace("--- Commit of offsets now is {},commitTimeoutMs is {}", now,commitTimeoutMs);
            // Check for timed out commits
            if (committing && now >= commitTimeoutMs) {
                logger.warn("--- {} Commit of offsets timed out", this);
                commitFailures++;
                committing = false;
            }
            // And process messages
            long timeoutMs = Math.max(nextCommit - now, 0);
            logger.trace("--- poll timeoutMS {},now is {},nextCommit is {}", timeoutMs,now,nextCommit);
            poll(timeoutMs);

        } catch (WakeupException we) {
            logger.error("{} Consumer woken up", this);
            if (isStopping())
                return;
            if (shouldPause()) {
                pauseAll();
                onPause();
                context.requestCommit();
            } else if (!pausedForRedelivery) {
                resumeAll();
                onResume();
            }
        }
    }

    protected void poll(long timeoutMs) {
        rewind();
        long retryTimeout = context.timeout();
        if (retryTimeout > 0) {
            timeoutMs = Math.min(timeoutMs, retryTimeout);
            context.timeout(-1L);
        }

        logger.trace("---{} Polling consumer with timeout {} ms", this, timeoutMs);
        ConsumerRecords<byte[], byte[]> msgs = pollConsumer(timeoutMs);
        assert messageBatch.isEmpty() || msgs.isEmpty();
        logger.trace("---{} Polling returned {} messages,detail is {}", this, msgs.count(),msgs);
        if(!shouldPause()) { //started
            convertMessages(msgs);
            deliverMessages();
        }
        dispatcher(timeoutMs);

    }


    private void dispatcher(long timeoutMs) {
        //notPausedForDispatcher = false;
        //lastPausedForDispatcher = pausedForDispatcher;
        logger.trace("---{} Dispatcher batch of {} messages to task thread", this, messageBatch.size());
        notPausedForDispatcher = queueManager.addMessage(new ArrayList<>(messageBatch));
        logger.trace("---{} Dispatcher add batch of messages to task thread result is {}", this, notPausedForDispatcher);
        if (!notPausedForDispatcher) {// add queue fail ,queue is overload
            if (!shouldPause()) {
                pauseAll();
                onPause();
                super.transitionTo(TargetState.PAUSED);
            }
            //messageBatch.clear();
            /*this.reentrantLock.lock();
            try {
                waitLock.await(timeoutMs-1L,TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                logger.error("--write lock is exception:", e);
            } finally {
                this.reentrantLock.unlock();
            }*/
            logger.trace("---main thread paused");
            return;
        }

        if(notPausedForDispatcher && shouldPause()){
            resumeAll();
            onResume();
            super.transitionTo(TargetState.STARTED);
            notPausedForDispatcher = true;
        }

        messageBatch.clear();

    }

    private void rewind() {
        Map<TopicPartition, Long> offsets = context.offsets();
        logger.trace("{} Rewind {} to offset", this, offsets.toString());
        if (offsets.isEmpty()) {
            return;
        }
        for (Map.Entry<TopicPartition, Long> entry: offsets.entrySet()) {
            TopicPartition tp = entry.getKey();
            Long offset = entry.getValue();
            if (offset != null) {
                logger.trace("{} Rewind {} to offset {}", this, tp, offset);
                consumer.seek(tp, offset);
                lastCommittedOffsets.put(tp, new OffsetAndMetadata(offset));
                currentOffsets.put(tp, new OffsetAndMetadata(offset));
            } else {
                logger.warn("{} Cannot rewind {} to null offset", this, tp);
            }
        }
        context.clearOffsets();
    }

    private ConsumerRecords<byte[], byte[]> pollConsumer(long timeoutMs) {
        ConsumerRecords<byte[], byte[]> msgs = consumer.poll(Duration.ofMillis(timeoutMs));

        // Exceptions raised from the task during a rebalance should be rethrown to stop the worker
        if (rebalanceException != null) {
            RuntimeException e = rebalanceException;
            rebalanceException = null;
            throw e;
        }

        sinkTaskMetricsGroup.recordRead(msgs.count());
        return msgs;
    }

    private void deliverMessages() {
        // Finally, deliver this batch to the sink
        try {
            // Since we reuse the messageBatch buffer, ensure we give the task its own copy
            logger.trace("{} Delivering batch of {} messages to task", this, messageBatch.size());
            long start = time.milliseconds();
            sinkTask.put(new ArrayList<>(messageBatch));
            // if errors raised from the operator were swallowed by the task implementation, an
            // exception needs to be thrown to kill the task indicating the tolerance was exceeded
            if (retryWithToleranceOperator.failed() && !retryWithToleranceOperator.withinToleranceLimits()) {
                throw new ConnectException("Tolerance exceeded in error handler",
                        retryWithToleranceOperator.error());
            }
            //recordBatch(messageBatch.size());
            sinkTaskMetricsGroup.recordPut(time.milliseconds() - start);
            currentOffsets.putAll(origOffsets);
            //messageBatch.clear();
            // If we had paused all consumer topic partitions to try to redeliver data, then we should resume any that
            // the task had not explicitly paused
            if (pausedForRedelivery) {
                if (!shouldPause())
                    resumeAll();
                pausedForRedelivery = false;
            }
        } catch (RetriableException e) {
            logger.error("{} RetriableException from SinkTask:", this, e);
            // If we're retrying a previous batch, make sure we've paused all topic partitions so we don't get new data,
            // but will still be able to poll in order to handle user-requested timeouts, keep group membership, etc.
            pausedForRedelivery = true;
            pauseAll();
            // Let this exit normally, the batch will be reprocessed on the next loop.
        } catch (Throwable t) {
            logger.error("{} Task threw an uncaught and unrecoverable exception. Task is being killed and will not "
                    + "recover until manually restarted. Error: {}", this, t.getMessage(), t);
            throw new ConnectException("Exiting WorkerSinkTask due to unrecoverable exception.", t);
        }
    }

    private void convertMessages(ConsumerRecords<byte[], byte[]> msgs) {
        origOffsets.clear();
        for (ConsumerRecord<byte[], byte[]> msg : msgs) {

            logger.trace("{} Consuming and converting message in topic '{}' partition {} at offset {} and timestamp {}",
                    this, msg.topic(), msg.partition(), msg.offset(), msg.timestamp());

            retryWithToleranceOperator.consumerRecord(msg);

            InternalSinkRecord transRecord = convertAndTransformRecord(msg);

            origOffsets.put(
                    new TopicPartition(msg.topic(), msg.partition()),
                    new OffsetAndMetadata(msg.offset() + 1)
            );
            if (transRecord != null) {
                messageBatch.add(transRecord);
            } else {
                logger.trace(
                        "{} Converters and transformations returned null, possibly because of too many retries, so " +
                                "dropping record in topic '{}' partition {} at offset {}",
                        this, msg.topic(), msg.partition(), msg.offset()
                );
            }
        }
        sinkTaskMetricsGroup.recordConsumedOffsets(origOffsets);
    }

    private InternalSinkRecord convertAndTransformRecord(final ConsumerRecord<byte[], byte[]> msg) {
        SchemaAndValue keyAndSchema = retryWithToleranceOperator.execute(() -> convertKey(msg),
                Stage.KEY_CONVERTER, keyConverter.getClass());

        SchemaAndValue valueAndSchema = retryWithToleranceOperator.execute(() -> convertValue(msg),
                Stage.VALUE_CONVERTER, valueConverter.getClass());

        Headers headers = retryWithToleranceOperator.execute(() -> convertHeadersFor(msg), Stage.HEADER_CONVERTER, headerConverter.getClass());

        if (retryWithToleranceOperator.failed()) {
            return null;
        }

        Long timestamp = ConnectUtils.checkAndConvertTimestamp(msg.timestamp());
        SinkRecord origRecord = new SinkRecord(valueAndSchema.schema().name(), msg.partition(),
                keyAndSchema.schema(), keyAndSchema.value(),
                valueAndSchema.schema(), valueAndSchema.value(),
                msg.offset(),
                timestamp,
                msg.timestampType(),
                headers);
        logger.trace("{} Applying transformations to record in topic '{}' partition {} at offset {} and timestamp {} with key {} and value {}",
                this, msg.topic(), msg.partition(), msg.offset(), timestamp, keyAndSchema.value(), valueAndSchema.value());
        /*@1
        if (isTopicTrackingEnabled) {
            recordActiveTopic(origRecord.topic());
        }*/

        // Apply the transformations
        SinkRecord transformedRecord = transformationChain.apply(origRecord);
        if (transformedRecord == null) {
            return null;
        }
        // Error reporting will need to correlate each sink record with the original consumer record
        return new InternalSinkRecord(msg, transformedRecord);
    }


    private SchemaAndValue convertKey(ConsumerRecord<byte[], byte[]> msg) {
        try {
            return keyConverter.toConnectData(msg.topic(), msg.headers(), (byte[])msg.key());
        } catch (DataException e) {
            logger.error("{} Error converting message key in topic '{}' partition {} at offset {} and timestamp {}: {}",
                    this, msg.topic(), msg.partition(), msg.offset(), msg.timestamp(), e.getMessage());
            throw e;
        }
    }

    private SchemaAndValue convertValue(ConsumerRecord<byte[], byte[]> msg) {
        try {
            return valueConverter.toConnectData(msg.topic(), msg.headers(), (byte[])msg.value());
        } catch (DataException e) {
            logger.error("{} Error converting message value in topic '{}' partition {} at offset {} and timestamp {}: {}",
                    this, msg.topic(), msg.partition(), msg.offset(), msg.timestamp(), e.getMessage());
            throw e;
        }
    }

    private Headers convertHeadersFor(ConsumerRecord<byte[], byte[]> record) {
        Headers result = new ConnectHeaders();
        org.apache.kafka.common.header.Headers recordHeaders = record.headers();
        if (recordHeaders != null) {
            String topic = record.topic();
            for (org.apache.kafka.common.header.Header recordHeader : recordHeaders) {
                SchemaAndValue schemaAndValue = headerConverter.toConnectHeader(topic, recordHeader.key(), recordHeader.value());
                result.add(recordHeader.key(), schemaAndValue);
            }
        }
        return result;
    }

    private void commitOffsets(long now, boolean closing) {
        if (currentOffsets.isEmpty())
            return;

        committing = true;
        commitSeqno += 1;
        commitStarted = now;
        sinkTaskMetricsGroup.recordOffsetSequenceNumber(commitSeqno);

        final Map<TopicPartition, OffsetAndMetadata> taskProvidedOffsets;
        try {
            logger.trace("{} Calling task.preCommit with current offsets: {}", this, currentOffsets);
            taskProvidedOffsets = sinkTask.preCommit(new HashMap<>(currentOffsets));
        } catch (Throwable t) {
            if (closing) {
                logger.warn("{} Offset commit failed during close", this);
                onCommitCompleted(t, commitSeqno, null);
            } else {
                logger.error("{} Offset commit failed, rewinding to last committed offsets", this, t);
                for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : lastCommittedOffsets.entrySet()) {
                    logger.debug("{} Rewinding topic partition {} to offset {}", this, entry.getKey(), entry.getValue().offset());
                    consumer.seek(entry.getKey(), entry.getValue().offset());
                }
                currentOffsets = new HashMap<>(lastCommittedOffsets);
                onCommitCompleted(t, commitSeqno, null);
            }
            return;

        } finally {
            if (closing) {
                logger.trace("{} Closing the task before committing the offsets: {}", this, currentOffsets);
                sinkTask.close(currentOffsets.keySet());
            }
        }

        if (taskProvidedOffsets.isEmpty()) {
            logger.debug("{} Skipping offset commit, task opted-out by returning no offsets from preCommit", this);
            onCommitCompleted(null, commitSeqno, null);
            return;
        }

        final Map<TopicPartition, OffsetAndMetadata> commitableOffsets = new HashMap<>(lastCommittedOffsets);
        for (Map.Entry<TopicPartition, OffsetAndMetadata> taskProvidedOffsetEntry : taskProvidedOffsets.entrySet()) {
            final TopicPartition partition = taskProvidedOffsetEntry.getKey();
            final OffsetAndMetadata taskProvidedOffset = taskProvidedOffsetEntry.getValue();
            if (commitableOffsets.containsKey(partition)) {
                long taskOffset = taskProvidedOffset.offset();
                long currentOffset = currentOffsets.get(partition).offset();
                if (taskOffset <= currentOffset) {
                    commitableOffsets.put(partition, taskProvidedOffset);
                } else {
                    logger.warn("{} Ignoring invalid task provided offset {}/{} -- not yet consumed, taskOffset={} currentOffset={}",
                            this, partition, taskProvidedOffset, taskOffset, currentOffset);
                }
            } else {
                logger.warn("{} Ignoring invalid task provided offset {}/{} -- partition not assigned, assignment={}",
                        this, partition, taskProvidedOffset, consumer.assignment());
            }
        }

        if (commitableOffsets.equals(lastCommittedOffsets)) {
            logger.debug("{} Skipping offset commit, no change since last commit", this);
            onCommitCompleted(null, commitSeqno,null);
            //onCommitCompleted(null, commitSeqno, null);
            return;
        }

        //doCommit(commitableOffsets, closing, commitSeqno);
        doCommit(commitableOffsets, closing,commitSeqno);
    }

    private void doCommit(Map<TopicPartition, OffsetAndMetadata> offsets, boolean closing,int seqno) {
        if (closing) {
            doCommitSync(offsets,seqno);
        } else {
            doCommitAsync(offsets,seqno);
        }
    }

    private void doCommitSync(Map<TopicPartition, OffsetAndMetadata> offsets ,int seqno) {
        logger.debug("{} Committing offsets synchronously using sequence number {}: {}", this, 0, offsets);
        try {
            consumer.commitSync(offsets);
            onCommitCompleted(null, seqno, offsets);
        } catch (WakeupException e) {
            // retry the commit to ensure offsets get pushed, then propagate the wakeup up to poll
            doCommitSync(offsets,seqno);
            throw e;
        } catch (KafkaException e) {
            onCommitCompleted(e,  seqno,offsets);
        }
    }

    private void doCommitAsync(Map<TopicPartition, OffsetAndMetadata> offsets,int seqno) {
        logger.info("{} Committing offsets asynchronously using sequence number {}: {}", this, 0, offsets);
        OffsetCommitCallback cb = new OffsetCommitCallback() {
            @Override
            public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception error) {
                onCommitCompleted(error,seqno,offsets);
            }
        };
        consumer.commitAsync(offsets, cb);
    }


    private void onCommitCompleted(Throwable error,long seqno, Map<TopicPartition, OffsetAndMetadata> committedOffsets) {
        if (commitSeqno != seqno) {
            logger.debug("{} Received out of order commit callback for sequence number {}, but most recent sequence number is {}",
                    this, seqno, commitSeqno);
            sinkTaskMetricsGroup.recordOffsetCommitSkip();
        } else {
            long durationMillis = time.milliseconds() - commitStarted;
            if (error != null) {
                logger.error("{} Commit of offsets threw an unexpected exception for sequence number {}: {}",
                        this, seqno, committedOffsets, error);
                commitFailures++;
                recordCommitFailure(durationMillis, error);
            } else {
                logger.debug("{} Finished offset commit successfully in {} ms for sequence number {}: {}",
                        this, durationMillis, seqno, committedOffsets);
                if (committedOffsets != null) {
                    logger.debug("{} Setting last committed offsets to {}", this, committedOffsets);
                    lastCommittedOffsets = committedOffsets;
                    sinkTaskMetricsGroup.recordCommittedOffsets(committedOffsets);
                }
                commitFailures = 0;
                recordCommitSuccess(durationMillis);
            }
            committing = false;
        }
    }

    /*
     *Rebalance三种情况
     * 1、group加入新的consumer
     * 2、consumer heartbeat 超时断开
     * 3、加入或者删除 partition
     */
    private class HandleRebalance implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            logger.debug("{} Partitions assigned {}", TenetWorkerTask.this, partitions);
            lastCommittedOffsets = new HashMap<>();
            currentOffsets = new HashMap<>();
            for (TopicPartition tp : partitions) {
                long pos = consumer.position(tp);
                lastCommittedOffsets.put(tp, new OffsetAndMetadata(pos));
                currentOffsets.put(tp, new OffsetAndMetadata(pos));
                logger.debug("{} Assigned topic partition {} with offset {}", TenetWorkerTask.this, tp, pos);
            }
            sinkTaskMetricsGroup.assignedOffsets(currentOffsets);

            // If we paused everything for redelivery (which is no longer relevant since we discarded the data), make
            // sure anything we paused that the task didn't request to be paused *and* which we still own is resumed.
            // Also make sure our tracking of paused partitions is updated to remove any partitions we no longer own.

            pausedForRedelivery = false;

            // Ensure that the paused partitions contains only assigned partitions and repause as necessary
            context.pausedPartitions().retainAll(partitions);
            if (shouldPause())
                pauseAll();
            else if (!context.pausedPartitions().isEmpty())
                consumer.pause(context.pausedPartitions());

            // Instead of invoking the assignment callback on initialization, we guarantee the consumer is ready upon
            // task start. Since this callback gets invoked during that initial setup before we've started the task, we
            // need to guard against invoking the user's callback method during that period.
            if (rebalanceException == null || rebalanceException instanceof WakeupException) {
                try {
                    openPartitions(partitions);
                    // Rewind should be applied only if openPartitions succeeds.
                    rewind();
                } catch (RuntimeException e) {
                    // The consumer swallows exceptions raised in the rebalance listener, so we need to store
                    // exceptions and rethrow when poll() returns.
                    rebalanceException = e;
                }
            }
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            if (taskStopped) {
                logger.trace("Skipping partition revocation callback as task has already been stopped");
                return;
            }
            //logger.debug("{} Partitions revoked", WorkerSinkTask.this);
            logger.debug("{} Partitions revoked", TenetWorkerTask.this);
            try {
                closePartitions();
                sinkTaskMetricsGroup.clearOffsets();
            } catch (RuntimeException e) {
                // The consumer swallows exceptions raised in the rebalance listener, so we need to store
                // exceptions and rethrow when poll() returns.
                rebalanceException = e;
            }

            // Make sure we don't have any leftover data since offsets will be reset to committed positions
            messageBatch.clear();
        }
    }

    private void pauseAll() {
        logger.debug("--pauseAll do thread {}",Thread.currentThread().getName());
        consumer.pause(consumer.assignment());
    }

    private void openPartitions(Collection<TopicPartition> partitions) {
        sinkTaskMetricsGroup.recordPartitionCount(partitions.size());
        sinkTask.open(partitions);
    }

    private void closePartitions() {
        logger.debug("--closePartitions do thread {}",Thread.currentThread().getName());
        commitOffsets(time.milliseconds(), true);
        sinkTaskMetricsGroup.recordPartitionCount(0);
    }

    private void resumeAll() {
        logger.debug("--resumeAll");
        for (TopicPartition tp : consumer.assignment())
            if (!context.pausedPartitions().contains(tp))
                consumer.resume(singleton(tp));
    }

    @Override
    protected void recordCommitFailure(long duration, Throwable error) {
        super.recordCommitFailure(duration, error);
    }

    @Override
    protected void recordCommitSuccess(long duration) {
        super.recordCommitSuccess(duration);
        sinkTaskMetricsGroup.recordOffsetCommitSuccess();
    }

    @Override
    protected void recordBatch(int size) {
        super.recordBatch(size);
        sinkTaskMetricsGroup.recordSend(size);
    }

    @FunctionalInterface
    public interface UncheckedCloseable extends AutoCloseable {
        @Override
        void close();
    }

    static class SinkTaskMetricsGroup {
        private final TenetTaskId id;
        private final ConnectMetrics metrics;
        private final ConnectMetrics.MetricGroup metricGroup;
        private final Sensor sinkRecordRead;
        private final Sensor sinkRecordSend;
        private final Sensor partitionCount;
        private final Sensor offsetSeqNum;
        private final Sensor offsetCompletion;
        private final Sensor offsetCompletionSkip;
        private final Sensor putBatchTime;
        private final Sensor sinkRecordActiveCount;
        private long activeRecords;
        private Map<TopicPartition, OffsetAndMetadata> consumedOffsets = new HashMap<>();
        private Map<TopicPartition, OffsetAndMetadata> committedOffsets = new HashMap<>();

        public SinkTaskMetricsGroup(TenetTaskId id, ConnectMetrics connectMetrics) {
            this.metrics = connectMetrics;
            this.id = id;

            ConnectMetricsRegistry registry = connectMetrics.registry();
            metricGroup = connectMetrics
                    .group(registry.sinkTaskGroupName(), registry.connectorTagName(), id.connector(), registry.taskTagName(),
                            Integer.toString(id.task()));
            // prevent collisions by removing any previously created metrics in this group.
            metricGroup.close();

            sinkRecordRead = metricGroup.sensor("sink-record-read");
            sinkRecordRead.add(metricGroup.metricName(registry.sinkRecordReadRate), new Rate());
            sinkRecordRead.add(metricGroup.metricName(registry.sinkRecordReadTotal), new CumulativeSum());

            sinkRecordSend = metricGroup.sensor("sink-record-send");
            sinkRecordSend.add(metricGroup.metricName(registry.sinkRecordSendRate), new Rate());
            sinkRecordSend.add(metricGroup.metricName(registry.sinkRecordSendTotal), new CumulativeSum());

            sinkRecordActiveCount = metricGroup.sensor("sink-record-active-count");
            sinkRecordActiveCount.add(metricGroup.metricName(registry.sinkRecordActiveCount), new Value());
            sinkRecordActiveCount.add(metricGroup.metricName(registry.sinkRecordActiveCountMax), new Max());
            sinkRecordActiveCount.add(metricGroup.metricName(registry.sinkRecordActiveCountAvg), new Avg());

            partitionCount = metricGroup.sensor("partition-count");
            partitionCount.add(metricGroup.metricName(registry.sinkRecordPartitionCount), new Value());

            offsetSeqNum = metricGroup.sensor("offset-seq-number");
            offsetSeqNum.add(metricGroup.metricName(registry.sinkRecordOffsetCommitSeqNum), new Value());

            offsetCompletion = metricGroup.sensor("offset-commit-completion");
            offsetCompletion.add(metricGroup.metricName(registry.sinkRecordOffsetCommitCompletionRate), new Rate());
            offsetCompletion.add(metricGroup.metricName(registry.sinkRecordOffsetCommitCompletionTotal), new CumulativeSum());

            offsetCompletionSkip = metricGroup.sensor("offset-commit-completion-skip");
            offsetCompletionSkip.add(metricGroup.metricName(registry.sinkRecordOffsetCommitSkipRate), new Rate());
            offsetCompletionSkip.add(metricGroup.metricName(registry.sinkRecordOffsetCommitSkipTotal), new CumulativeSum());

            putBatchTime = metricGroup.sensor("put-batch-time");
            putBatchTime.add(metricGroup.metricName(registry.sinkRecordPutBatchTimeMax), new Max());
            putBatchTime.add(metricGroup.metricName(registry.sinkRecordPutBatchTimeAvg), new Avg());
        }

        void computeSinkRecordLag() {
            Map<TopicPartition, OffsetAndMetadata> consumed = this.consumedOffsets;
            Map<TopicPartition, OffsetAndMetadata> committed = this.committedOffsets;
            activeRecords = 0L;
            for (Map.Entry<TopicPartition, OffsetAndMetadata> committedOffsetEntry : committed.entrySet()) {
                final TopicPartition partition = committedOffsetEntry.getKey();
                final OffsetAndMetadata consumedOffsetMeta = consumed.get(partition);
                if (consumedOffsetMeta != null) {
                    final OffsetAndMetadata committedOffsetMeta = committedOffsetEntry.getValue();
                    long consumedOffset = consumedOffsetMeta.offset();
                    long committedOffset = committedOffsetMeta.offset();
                    long diff = consumedOffset - committedOffset;
                    // Connector tasks can return offsets, so make sure nothing wonky happens
                    activeRecords += Math.max(diff, 0L);
                }
            }
            sinkRecordActiveCount.record(activeRecords);
        }

        void close() {
            metricGroup.close();
        }

        void recordRead(int batchSize) {
            sinkRecordRead.record(batchSize);
        }

        void recordSend(int batchSize) {
            sinkRecordSend.record(batchSize);
        }

        void recordPut(long duration) {
            putBatchTime.record(duration);
        }

        void recordPartitionCount(int assignedPartitionCount) {
            partitionCount.record(assignedPartitionCount);
        }

        void recordOffsetSequenceNumber(int seqNum) {
            offsetSeqNum.record(seqNum);
        }

        void recordConsumedOffsets(Map<TopicPartition, OffsetAndMetadata> offsets) {
            consumedOffsets.putAll(offsets);
            computeSinkRecordLag();
        }

        void recordCommittedOffsets(Map<TopicPartition, OffsetAndMetadata> offsets) {
            committedOffsets = offsets;
            computeSinkRecordLag();
        }

        void assignedOffsets(Map<TopicPartition, OffsetAndMetadata> offsets) {
            consumedOffsets = new HashMap<>(offsets);
            committedOffsets = offsets;
            sinkRecordActiveCount.record(0.0);
        }

        void clearOffsets() {
            consumedOffsets.clear();
            committedOffsets.clear();
            sinkRecordActiveCount.record(0.0);
        }

        void recordOffsetCommitSuccess() {
            offsetCompletion.record(1.0);
        }

        void recordOffsetCommitSkip() {
            offsetCompletionSkip.record(1.0);
        }

        protected ConnectMetrics.MetricGroup metricGroup() {
            return metricGroup;
        }
    }

    @Override
    public void removeMetrics() {
        try {
            sinkTaskMetricsGroup.close();
        } finally {
            super.removeMetrics();
        }
    }

}
