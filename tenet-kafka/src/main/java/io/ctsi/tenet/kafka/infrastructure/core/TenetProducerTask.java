package io.ctsi.tenet.kafka.infrastructure.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.ctsi.tenet.kafka.config.TenetKafkaProperties;
import io.ctsi.tenet.kafka.connect.ConnectHeaders;
import io.ctsi.tenet.kafka.connect.Header;
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
import io.ctsi.tenet.kafka.connect.sink.*;
import io.ctsi.tenet.kafka.connect.source.SourceRecord;
import io.ctsi.tenet.kafka.connect.sink.WorkerTask;
import io.ctsi.tenet.kafka.task.BufferManager;
import io.ctsi.tenet.kafka.mongodb.util.ConnectUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.Value;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class TenetProducerTask extends WorkerTask {
    private static final Logger log = LoggerFactory.getLogger(TenetProducerTask.class);

    private final SinkTask sinkTask;
    private final Converter keyConverter;
    private final Converter valueConverter;
    private final HeaderConverter headerConverter;
    private final TransformationChain<SinkRecord> transformationChain;
    private KafkaProducer<byte[], byte[]> producer;
    private final AtomicReference<Exception> producerSendException;
    private List<String> toSend;
    private boolean lastSendFailed; // Whether the last send failed *synchronously*, i.e. never made it into the producer's RecordAccumulator
    //private CountDownLatch stopRequestedLatch;
    private Map<String, String> taskConfig;
    //private boolean finishedStart = false;
    //private boolean startedShutdownBeforeStartCompleted = false;
    private boolean stopped = false;
    private final Time time;
    private final BufferManager bufferManager;
    private final List<SinkRecord> messageBatch;
    private final SourceTaskMetricsGroup sourceTaskMetricsGroup;
    private ObjectMapper mapper = new ObjectMapper();
    private String targetTopic = "";
    private static final String SEND_RESULT = "send_result";

    public TenetProducerTask(TenetTaskId id,
                             SinkTask sinkTask,
                             TaskStatus.Listener statusListener,
                             TargetState initialState,
                             Converter keyConverter,
                             Converter valueConverter,
                             HeaderConverter headerConverter,
                             TransformationChain<SinkRecord> transformationChain,
                             KafkaProducer<byte[], byte[]> producer,
                             Time time,
                             ConnectMetrics connectMetrics,
                             RetryWithToleranceOperator retryWithToleranceOperator,
                             MemoryStatusBackingStore memoryStatusBackingStore,
                             BufferManager bufferManager) {

        super(id, statusListener, initialState, connectMetrics, retryWithToleranceOperator, time, memoryStatusBackingStore);
        this.sinkTask = sinkTask;
        this.keyConverter = keyConverter;
        this.valueConverter = valueConverter;
        this.headerConverter = headerConverter;
        this.transformationChain = transformationChain;
        this.producer = producer;
        this.toSend = null;
        this.lastSendFailed = false;
        this.messageBatch = new CopyOnWriteArrayList<>();
        // this.stopRequestedLatch = new CountDownLatch(1);
        this.producerSendException = new AtomicReference<>();
        this.time = time;
        this.bufferManager = bufferManager;
        this.sourceTaskMetricsGroup = new SourceTaskMetricsGroup(id, connectMetrics);
    }

    @Override
    public void initialize(TaskConfig taskConfig) {
        try {
            this.taskConfig = taskConfig.originalsStrings();
            if (this.taskConfig.get("dialectName").startsWith("Mongo")) {
                this.taskConfig.put("topic_suffix", "_out");
                this.taskConfig.put("topics", this.taskConfig.get("tenet.target.topic"));
            }
            int max = Integer.valueOf(this.taskConfig.get("tenet.max.queue"));
            targetTopic = this.taskConfig.get("tenet.target.topic") != null
                    ? this.taskConfig.get("tenet.target.topic") : "";
            //TODO
            //bufferManager.init(max,producer,this.taskConfig.get("tenet.target.topic").toString(),this.valueConverter);
            bufferManager.init(max);
        } catch (Throwable t) {
            log.error("{} Task failed initialization and will not be started.", this, t);
            onFailure(t);
            throw new RuntimeException(t);
        }
    }

    /**
     * Initializes and starts the SinkTask.
     */
    public void initializeAndStart() {
        TenetKafkaProperties.validate(taskConfig);
        sinkTask.initialize(new TenetWorkerTaskContext(null, null));
        sinkTask.start(taskConfig);
        log.info("{} Producer Sink task finished initialization and start", this);
    }

    @Override
    protected void close() {
        log.info("--- Close TenetProducerTask message size is {}", bufferManager.getCtrl());
        try {
            for (int i = 0; i < 5; ) {
                log.info("--- Close TenetProducerTask wait buffer change to empty");
                if (bufferManager.getCtrl() == 0)
                    i++;
                Thread.currentThread().sleep(1000);
            }
        } catch (Exception e) {
            log.error("shutdown error is :", e);
        }

        if (!shouldPause()) {
            tryStop();
        }
        if (producer != null) {
            try {
                producer.close(Duration.ofSeconds(30));
            } catch (Throwable t) {
                log.warn("Could not close producer", t);
            }
        }
        try {
            transformationChain.close();
        } catch (Throwable t) {
            log.warn("Could not close transformation chain", t);
        }
        Utils.closeQuietly(retryWithToleranceOperator, "retry operator");
    }


    @Override
    public void cancel() {
        super.cancel();
    }

    @Override
    public void stop() {
        super.stop();
        //tryStop();
        /*stopRequestedLatch.countDown();
        synchronized (this) {
            if (finishedStart)
                tryStop();
            else
                startedShutdownBeforeStartCompleted = true;
        }*/
    }

    private synchronized void tryStop() {
        if (!stopped) {
            try {
                sinkTask.stop();
                stopped = true;
            } catch (Throwable t) {
                log.warn("Could not stop task", t);
            }
        }
    }

    @Override
    public void execute() {

        initializeAndStart();
        log.info("{} Producer task finished initialization and start", this);

        while (!isStopping()) {
            maybeThrowProducerSendException();
            if (toSend == null) {
                log.info("{} Nothing to send to Kafka. Polling source for additional records", this);
                long start = time.milliseconds();
                toSend = poll();
                if (toSend != null && !toSend.isEmpty()) {
                    recordPollReturned(toSend.size(), time.milliseconds() - start);
                }
            }
            if (toSend == null || toSend.isEmpty()) {
                toSend = null;
                continue;
            }
            sendRecords();
        }


    }

    private void maybeThrowProducerSendException() {
        if (producerSendException.get() != null) {
            throw new ConnectException(
                    "Unrecoverable exception from producer send callback",
                    producerSendException.get()
            );
        }
    }

    protected List<String> poll() {
        try {
            //List<ConsumerRecord<byte[],byte[]>> msgs = this.bufferManager.fetchData();//return null;
            //return null;
            //convertMessages(msgs);
            //maybeThrowProducerSendException();
            //sendRecords();
            //deliverMessages();
            return this.bufferManager.fetchData();
        } catch (RetriableException  e) {
            log.warn("{} failed to poll records from SourceTask. Will retry operation.", this, e);
            // Do nothing. Let the framework poll whenever it's ready.
            return null;
        }
    }

    protected void recordPollReturned(int numRecordsInBatch, long duration) {
        sourceTaskMetricsGroup.recordPoll(numRecordsInBatch, duration);
    }

    private boolean sendRecords() {
        //int processed = 0;
        recordBatch(toSend.size());
        final SourceRecordWriteCounter counter =
                toSend.size() > 0 ? new SourceRecordWriteCounter(toSend.size(), sourceTaskMetricsGroup) : null;
        CountDownLatch countDownLatch = new CountDownLatch(toSend.size());
        for (final String preTransformRecord : toSend) {
            maybeThrowProducerSendException();

            //retryWithToleranceOperator.sourceRecord(preTransformRecord);
            //final SourceRecord record = transformationChain.apply(preTransformRecord);
            final JsonNode jsonNode;
            try {
                jsonNode = mapper.readTree(preTransformRecord);
            } catch (JsonProcessingException e) {
                log.error("---process business result is not a json ", e);
                continue;
            }
            final ProducerRecord<byte[], byte[]> producerRecord = convertTransformedRecord(jsonNode);
            if (producerRecord == null || retryWithToleranceOperator.failed()) {
                counter.skipRecord();
                // commitTaskRecord(preTransformRecord, null);
                continue;
            }

            log.trace("--- {} Appending record with key {}, value {}", this, producerRecord.key(), producerRecord.value());
            try {
                final String topic = targetTopic;
                final List<SinkRecord> messageBatch = this.messageBatch;

                producer.send(
                        producerRecord,
                        new Callback() {
                            @Override
                            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                                if (e != null) {
                                    log.error("--- {} failed to send record to {}:", TenetProducerTask.this, topic, e);
                                    log.debug("--- {} Failed record: {}", TenetProducerTask.this, preTransformRecord);
                                    producerSendException.compareAndSet(null, e);
                                } else {
                                    //recordSent(producerRecord);
                                    counter.completeRecord();
                                    log.trace("{} Wrote record successfully: topic {} partition {} offset {}",
                                            TenetProducerTask.this,
                                            recordMetadata.topic(), recordMetadata.partition(),
                                            recordMetadata.offset());
                                    //commitTaskRecord(preTransformRecord, recordMetadata);

                                }
                                ConsumerRecord<byte[],byte[]> record = saveToConsumer(recordMetadata, jsonNode,e);
                                messageBatch.add(convertTransformedRecord(record));
                                countDownLatch.countDown();
                            }
                        });

                lastSendFailed = false;
            } catch (RetriableException e) {
                log.warn("{} Failed to send {}, backing off before retrying:", this, producerRecord, e);
                //toSend = toSend.subList(processed, toSend.size());
                lastSendFailed = true;
                counter.retryRemaining();
                //return false;
            } catch (KafkaException e) {
                //throw new ConnectException("Unrecoverable exception trying to send", e);
            }
            //processed++;
        }
        try {
            countDownLatch.await(360, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.error("--- await send kafka timeout ");
            //TODO
            throw new Error();
        }
        deliverMessages();
        toSend.clear();
        toSend = null;
        return true;
    }

    private ConsumerRecord<byte[], byte[]> saveToConsumer(RecordMetadata recordMetadata, JsonNode jsonNode,Exception e) {

        String topic = recordMetadata.topic();//jsonNode.get("topic").asText();
        int partition = recordMetadata.partition();
        long offset = recordMetadata.offset();
        ObjectNode objectNode = JsonNodeFactory.instance.objectNode();
        Iterator<Map.Entry<String, JsonNode>> it = jsonNode.fields();
        Map.Entry<String, JsonNode> tmp;
        while (it.hasNext()) {
            tmp = it.next();
            objectNode.set(tmp.getKey(), tmp.getValue());
        }
        if (e != null) {
            //send error save
            objectNode.put(SEND_RESULT, false);
        } else {
            objectNode.put(SEND_RESULT, true);
        }
        ConsumerRecord<byte[], byte[]> consumerRecord = null;
        try {
            consumerRecord = new ConsumerRecord<byte[], byte[]>(topic, partition, offset, null, mapper.writeValueAsBytes(objectNode));
        } catch (IOException ie) {
            log.error("tenet kafka producer send result trans byte error:{}", e);
        }
        return consumerRecord;

    }

    private void deliverMessages() {
        // Finally, deliver this batch to the sink
        try {
            // Since we reuse the messageBatch buffer, ensure we give the task its own copy
            log.trace("--- {} Delivering batch of {} messages to task", this, messageBatch.size());
            //long start = time.milliseconds();
            sinkTask.put(new ArrayList<>(messageBatch));
            // if errors raised from the operator were swallowed by the task implementation, an
            // exception needs to be thrown to kill the task indicating the tolerance was exceeded
            if (retryWithToleranceOperator.failed() && !retryWithToleranceOperator.withinToleranceLimits()) {
                throw new ConnectException("Tolerance exceeded in error handler",
                        retryWithToleranceOperator.error());
            }

            messageBatch.clear();
            // If we had paused all consumer topic partitions to try to redeliver data, then we should resume any that
            // the task had not explicitly paused
            /*if (pausedForRedelivery) {
                if (!shouldPause())
                    resumeAll();
                pausedForRedelivery = false;
            }*/
        } catch (RetriableException e) {
            log.error("--- {} RetriableException from SinkTask:", this, e);
            // If we're retrying a previous batch, make sure we've paused all topic partitions so we don't get new data,
            // but will still be able to poll in order to handle user-requested timeouts, keep group membership, etc.
            /*
            pausedForRedelivery = true;
            pauseAll();
             */
            // Let this exit normally, the batch will be reprocessed on the next loop.
        } catch (Throwable t) {
            log.error("{} Task threw an uncaught and unrecoverable exception. Task is being killed and will not "
                    + "recover until manually restarted. Error: {}", this, t.getMessage(), t);
            throw new ConnectException("Exiting WorkerSinkTask due to unrecoverable exception.", t);
        }
    }

    /**
     * produce
     **/

    private ProducerRecord<byte[], byte[]> convertTransformedRecord(JsonNode record) {

        if (record == null) {
            return null;
        }

        //RetryWithToleranceOperator retryWithToleranceOperator = new RetryWithToleranceOperator(3, 5, ToleranceType.ALL, new SystemTime());
        byte[] value = retryWithToleranceOperator.execute(() -> valueConverter.fromConnectData(targetTopic, null, null, record),
                Stage.VALUE_CONVERTER, valueConverter.getClass());

        if (retryWithToleranceOperator.failed()) {
            return null;
        }
        return new ProducerRecord<>(targetTopic, value);

    }
    /**produce end**/


    /**save to db*/
    /**
     * Convert the source record into a producer record.
     *
     * @param msg the transformed record
     * @return the producer record which can sent over to Kafka. A null is returned if the input is null or
     * if an error was encountered during any of the converter stages.
     */
    private SinkRecord convertTransformedRecord(final ConsumerRecord<byte[], byte[]> msg) {
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
        log.trace("{} Applying transformations to record in topic '{}' partition {} at offset {} and timestamp {} with key {} and value {}",
                this, msg.topic(), msg.partition(), msg.offset(), timestamp, keyAndSchema.value(), valueAndSchema.value());

        SinkRecord transformedRecord = transformationChain.apply(origRecord);
        if (transformedRecord == null) {
            return null;
        }
        // Error reporting will need to correlate each sink record with the original consumer record
        return new InternalSinkRecord(msg, transformedRecord);
    }

    private RecordHeaders convertHeaderFor(SourceRecord record) {
        Headers headers = record.headers();
        RecordHeaders result = new RecordHeaders();
        if (headers != null) {
            String topic = record.topic();
            for (Header header : headers) {
                String key = header.key();
                byte[] rawHeader = headerConverter.fromConnectHeader(topic, key, header.schema(), header.value());
                result.add(key, rawHeader);
            }
        }
        return result;
    }

    private SchemaAndValue convertKey(ConsumerRecord<byte[], byte[]> msg) {
        try {
            return keyConverter.toConnectData(msg.topic(), msg.headers(), (byte[]) msg.key());
        } catch (Exception e) {
            log.error("{} Error converting message key in topic '{}' partition {} at offset {} and timestamp {}: {}",
                    this, msg.topic(), msg.partition(), msg.offset(), msg.timestamp(), e.getMessage(), e);
            throw e;
        }
    }

    private SchemaAndValue convertValue(ConsumerRecord<byte[], byte[]> msg) {
        try {
            return valueConverter.toConnectData(msg.topic(), msg.headers(), (byte[]) msg.value());
        } catch (Exception e) {
            log.error("{} Error converting message value in topic '{}' partition {} at offset {} and timestamp {}: {}",
                    this, msg.topic(), msg.partition(), msg.offset(), msg.timestamp(), e.getMessage(), e);
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

    @Override
    public String toString() {
        return "WorkerSourceTask{" +
                "id=" + id +
                '}';
    }

    @Override
    public void removeMetrics() {
        try {
            sourceTaskMetricsGroup.close();
        } finally {
            super.removeMetrics();
        }
    }

    static class SourceTaskMetricsGroup {
        private final ConnectMetrics.MetricGroup metricGroup;
        private final Sensor sourceRecordPoll;
        private final Sensor sourceRecordWrite;
        private final Sensor sourceRecordActiveCount;
        private final Sensor pollTime;
        private int activeRecordCount;

        public SourceTaskMetricsGroup(TenetTaskId id, ConnectMetrics connectMetrics) {
            ConnectMetricsRegistry registry = connectMetrics.registry();
            metricGroup = connectMetrics.group(registry.sourceTaskGroupName(),
                    registry.connectorTagName(), id.connector(),
                    registry.taskTagName(), Integer.toString(id.task()));
            // remove any previously created metrics in this group to prevent collisions.
            metricGroup.close();

            sourceRecordPoll = metricGroup.sensor("source-record-poll");
            sourceRecordPoll.add(metricGroup.metricName(registry.sourceRecordPollRate), new Rate());
            sourceRecordPoll.add(metricGroup.metricName(registry.sourceRecordPollTotal), new CumulativeSum());

            sourceRecordWrite = metricGroup.sensor("source-record-write");
            sourceRecordWrite.add(metricGroup.metricName(registry.sourceRecordWriteRate), new Rate());
            sourceRecordWrite.add(metricGroup.metricName(registry.sourceRecordWriteTotal), new CumulativeSum());

            pollTime = metricGroup.sensor("poll-batch-time");
            pollTime.add(metricGroup.metricName(registry.sourceRecordPollBatchTimeMax), new Max());
            pollTime.add(metricGroup.metricName(registry.sourceRecordPollBatchTimeAvg), new Avg());

            sourceRecordActiveCount = metricGroup.sensor("source-record-active-count");
            sourceRecordActiveCount.add(metricGroup.metricName(registry.sourceRecordActiveCount), new Value());
            sourceRecordActiveCount.add(metricGroup.metricName(registry.sourceRecordActiveCountMax), new Max());
            sourceRecordActiveCount.add(metricGroup.metricName(registry.sourceRecordActiveCountAvg), new Avg());
        }

        void close() {
            metricGroup.close();
        }

        void recordPoll(int batchSize, long duration) {
            sourceRecordPoll.record(batchSize);
            pollTime.record(duration);
            activeRecordCount += batchSize;
            sourceRecordActiveCount.record(activeRecordCount);
        }

        void recordWrite(int recordCount) {
            sourceRecordWrite.record(recordCount);
            activeRecordCount -= recordCount;
            activeRecordCount = Math.max(0, activeRecordCount);
            sourceRecordActiveCount.record(activeRecordCount);
        }

        protected ConnectMetrics.MetricGroup metricGroup() {
            return metricGroup;
        }
    }

    static class SourceRecordWriteCounter {
        private final SourceTaskMetricsGroup metricsGroup;
        private final int batchSize;
        private boolean completed = false;
        private int counter;

        public SourceRecordWriteCounter(int batchSize, SourceTaskMetricsGroup metricsGroup) {
            assert batchSize > 0;
            assert metricsGroup != null;
            this.batchSize = batchSize;
            counter = batchSize;
            this.metricsGroup = metricsGroup;
        }

        public void skipRecord() {
            if (counter > 0 && --counter == 0) {
                finishedAllWrites();
            }
        }

        public void completeRecord() {
            if (counter > 0 && --counter == 0) {
                finishedAllWrites();
            }
        }

        public void retryRemaining() {
            finishedAllWrites();
        }

        private void finishedAllWrites() {
            if (!completed) {
                metricsGroup.recordWrite(batchSize - counter);
                completed = true;
            }
        }
    }

    /*private void convertMessages(List<ConsumerRecord<byte[], byte[]>> msgs) {

        for (ConsumerRecord<byte[], byte[]> msg : msgs) {

            log.trace("{} Consuming and converting message in topic '{}' partition {} at offset {} and timestamp {}",
                    this, msg.topic(), msg.partition(), msg.offset(), msg.timestamp());

            retryWithToleranceOperator.consumerRecord(msg);

            SinkRecord transRecord = convertTransformedRecord(msg);

            if (transRecord != null) {
                messageBatch.add(transRecord);
            } else {
                log.trace(
                        "{} Converters and transformations returned null, possibly because of too many retries, so " +
                                "dropping record in topic '{}' partition {} at offset {}",
                        this, msg.topic(), msg.partition(), msg.offset()
                );
            }
        }
        msgs.clear();
        msgs = null;
        //sinkTaskMetricsGroup.recordConsumedOffsets(origOffsets);
    }*/

}
