package io.ctsi.tenet.kafka.infrastructure.core;

import io.ctsi.tenet.kafka.config.SchemaDescProperties;
import io.ctsi.tenet.kafka.config.TenetSinkProperties;
import io.ctsi.tenet.kafka.connect.ConnectRecord;
import io.ctsi.tenet.kafka.connect.TenetTaskId;
import io.ctsi.tenet.kafka.connect.json.JsonConverter;
import io.ctsi.tenet.kafka.connect.json.JsonConverterConfig;
import io.ctsi.tenet.kafka.connect.metrics.ConnectMetrics;
import io.ctsi.tenet.kafka.connect.metrics.ErrorHandlingMetrics;
import io.ctsi.tenet.kafka.connect.policy.RetryWithToleranceOperator;
import io.ctsi.tenet.kafka.connect.policy.ToleranceType;
import io.ctsi.tenet.kafka.connect.storage.MemoryStatusBackingStore;
import io.ctsi.tenet.kafka.connect.transforms.Flatten;
import io.ctsi.tenet.kafka.connect.transforms.Transformation;
import io.ctsi.tenet.kafka.connect.transforms.TransformationChain;
import io.ctsi.tenet.kafka.factory.ConsumerFactory;
import io.ctsi.tenet.kafka.factory.ProducerFactory;
import io.ctsi.tenet.kafka.infrastructure.config.TenetKafkaPropertiesBeanPostProcessor;
import io.ctsi.tenet.kafka.connect.data.Converter;
import io.ctsi.tenet.kafka.connect.data.HeaderConverter;
import io.ctsi.tenet.kafka.connect.data.StringConverter;
import io.ctsi.tenet.kafka.connect.sink.*;
import io.ctsi.tenet.kafka.mongodb.sink.MongoSinkTask;
import io.ctsi.tenet.kafka.task.BufferManager;
import io.ctsi.tenet.kafka.task.QueueManager;
import io.ctsi.tenet.kafka.task.util.TenetWorkerTaskConfigUtil;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.utils.Time;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;


public class TenetWorkerTaskFactory implements WorkerTaskFactory {

    private static final String SCHEMA_ENABLE = "schemas.enable";

    private SchemaDescProperties schemaDescProperties;

    private ConsumerFactory consumerFactory;

    private AtomicInteger count = new AtomicInteger(0);

    private TenetSinkProperties tenetSinkProperties;

    private  MemoryStatusBackingStore memoryStatusBackingStore; //= new MemoryStatusBackingStore();

    private QueueManager queueManager;

    private ConnectMetrics connectMetrics;

    private Time time;

    public void setTenetSinkProperties(TenetSinkProperties tenetSinkProperties) {
        this.tenetSinkProperties = tenetSinkProperties;
    }

    @Override
    public void setQueueManager(QueueManager queueManager) {
        this.queueManager = queueManager;
    }


    public void setSchemaDescConfig(SchemaDescProperties schemaDescProperties) {
        this.schemaDescProperties = schemaDescProperties;
    }

    @Override
    public void setMemoryStatusBackingStore(MemoryStatusBackingStore memoryStatusBackingStore) {
        this.memoryStatusBackingStore = memoryStatusBackingStore;
    }

    public void setConsumerFactory(ConsumerFactory consumerFactory) {
        this.consumerFactory = consumerFactory;
    }

    @Override
    public TenetWorkerTask createTask() {
        return this.createTenetWorkerTask();
    }
    @Override
    public void setTime(Time time) {
        this.time = time;
    }
    @Override
    public void setConnectMetrics(ConnectMetrics connectMetrics) {
        this.connectMetrics = connectMetrics;
    }

    private TenetWorkerTask createTenetWorkerTask() {

        KafkaConsumer<byte[], byte[]> consumer = consumerFactory.createConsumer();
        TenetTaskId tenetTaskId = new TenetTaskId(TenetWorkerTaskConfigUtil.GENERATED_ID_WORKER_PREFIX, count.getAndIncrement());
        RetryWithToleranceOperator retryWithToleranceOperator = new RetryWithToleranceOperator(3, 5, ToleranceType.ALL, time);
        retryWithToleranceOperator.metrics(new ErrorHandlingMetrics(tenetTaskId, connectMetrics));

        SinkTask sinkTask = null;
        List<Transformation<SinkRecord>> transformations = null;

        if (!this.tenetSinkProperties.getDialectName().startsWith("Mongo")) {
            //sinkTask = new JdbcSinkTask(); test for mysql
            transformations = getMysqlTransformations();
        } else {
            sinkTask = new MongoSinkTask();
            transformations = getMongoTransformations();
        }
        TransformationChain<SinkRecord> transformationChain = new TransformationChain<>(transformations, retryWithToleranceOperator);
        //schemaDescProperties.buildProperties();

        Converter keyConverter = getKeyConvert();
        Converter valueConverter = getValueConvert();
        HeaderConverter headerConverter = getHeaderConvert();

        TaskHerder taskHerder = new TaskHerder(memoryStatusBackingStore, tenetTaskId.connector() + tenetTaskId.task());
        TenetWorkerTask tenetWorkerTask = new TenetWorkerTask(tenetTaskId,
                sinkTask,
                taskHerder,
                TargetState.STARTED,
                keyConverter,
                valueConverter,
                headerConverter,
                transformationChain,
                consumer,
                time,
                connectMetrics,
                retryWithToleranceOperator,
                memoryStatusBackingStore,
                Long.valueOf(String.valueOf(consumerFactory.getConfigurationProperties().get(TaskConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG))),
                this.queueManager
        );
        return tenetWorkerTask;

    }

    private <R extends ConnectRecord<R>> List<Transformation<R>> getMysqlTransformations() {

        List<Transformation<R>> transformations = new ArrayList<>();
        Transformation<R> transformation = new Flatten.Value<R>();
        Map<String, String> map = new HashMap<>();
        transformation.configure(map);
        transformations.add(transformation);
        return transformations;

    }

    private <R extends ConnectRecord<R>> List<Transformation<R>> getMongoTransformations() {

        List<Transformation<R>> transformations = new ArrayList<>();
//        Transformation<R> transformation = new Flatten.Value<R>();
//        Map<String,String> map = new HashMap<>();
//        transformation.configure(map);
//        transformations.add(transformation);
        return transformations;

    }

    private Converter getKeyConvert() {

        Converter keyConverter = new StringConverter();
        Map<String, Boolean> map = new HashMap<>();
        map.put(SCHEMA_ENABLE, true);
        keyConverter.configure(map, true);
        return keyConverter;

    }

    private Converter getValueConvert() {

        Converter valueConverter = new JsonConverter();
        HashMap<String, Object> map = new HashMap<>();
        map.put(SCHEMA_ENABLE, false);
        map.put(JsonConverter.ROOT_NODE, schemaDescProperties.getInputJsonNode());//schemaDescProperties.buildProperties());
        map.put(JsonConverterConfig.OPR_TYPE, JsonConverterConfig.OprType.INPUT);
        valueConverter.configure(map, false);
        return valueConverter;

    }

    private HeaderConverter getHeaderConvert() {

        HeaderConverter headerConvert = new JsonConverter();
//        Map map = new HashMap<>();
//        map.put(SCHEMA_ENABLE,false);
//        headerConvert.configure(map);
        return headerConvert;

    }

    @Override
    public void setProducerFactory(ProducerFactory producerFactory) {
        // do nothing
    }

    @Override
    public void setBufferManager(BufferManager bufferManger) {
        //nothing

    }
}
