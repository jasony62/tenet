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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.utils.Time;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;


public class TenetProducerTaskFactory implements WorkerTaskFactory{

    private static final String SCHEMA_ENABLE = "schemas.enable";
    private SchemaDescProperties schemaDescProperties;
    private ProducerFactory producerFactory;
    private BufferManager bufferManager;
    private AtomicInteger count = new AtomicInteger(0);
    private Time time ;
    private ConnectMetrics connectMetrics;
    //private final MemoryStatusBackingStore memoryStatusBackingStore = new MemoryStatusBackingStore();
    private  MemoryStatusBackingStore memoryStatusBackingStore;

    private TenetSinkProperties tenetSinkProperties;

    public void setTenetSinkProperties(TenetSinkProperties tenetSinkProperties) {
        this.tenetSinkProperties = tenetSinkProperties;
    }

    public void setMemoryStatusBackingStore(MemoryStatusBackingStore memoryStatusBackingStore) {
        this.memoryStatusBackingStore = memoryStatusBackingStore;
    }

    @Override
    public void setTime(Time time) {
        this.time = time;
    }
    @Override
    public void setConnectMetrics(ConnectMetrics connectMetrics) {
        this.connectMetrics = connectMetrics;
    }

    public TenetProducerTask createTask() {
        KafkaProducer<byte[], byte[]> producer = producerFactory.createProducer();
        TenetTaskId tenetTaskId = new TenetTaskId(TenetWorkerTaskConfigUtil.GENERATED_ID_PRODUCER_PREFIX, count.getAndIncrement());
        //Time time = Time.SYSTEM;
        //ConnectMetrics connectMetrics = new ConnectMetrics(tenetTaskId.connector(), tenetSinkProperties.buildProperties(), time);
        RetryWithToleranceOperator retryWithToleranceOperator = new RetryWithToleranceOperator(3, 5, ToleranceType.ALL, time);
        //RetryWithToleranceOperator retryWithToleranceOperator = new RetryWithToleranceOperator(3, 5, ToleranceType.ALL, Time.SYSTEM);
        retryWithToleranceOperator.metrics(new ErrorHandlingMetrics(tenetTaskId, connectMetrics));

        SinkTask sinkTask = null;
        List<Transformation<SinkRecord>> transformations = null;
        if(!this.tenetSinkProperties.getDialectName().startsWith("Mongo")){
            //sinkTask = new JdbcSinkTask();
            transformations = getMysqlTransformations();
        }else{
            sinkTask = new MongoSinkTask();
            transformations = getMongoTransformations();
        }
        TransformationChain<SinkRecord> transformationChain = new TransformationChain<>(transformations, retryWithToleranceOperator);
        //schemaDescProperties.buildProperties();
        Converter keyConverter = getKeyConvert();
        Converter valueConverter = getValueConvert();
        HeaderConverter headerConverter = getHeaderConvert();

        TaskHerder taskHerder = new TaskHerder(memoryStatusBackingStore, tenetTaskId.connector() + tenetTaskId.task());

        return new TenetProducerTask(tenetTaskId,
                sinkTask,
                taskHerder,
                TargetState.STARTED,
                keyConverter,
                valueConverter,
                headerConverter,
                transformationChain,
                producer,
                time,
                connectMetrics,
                retryWithToleranceOperator,
                memoryStatusBackingStore,
                this.bufferManager);
    }

    public void setSchemaDescConfig(SchemaDescProperties schemaDescProperties) {
        this.schemaDescProperties = schemaDescProperties;
    }

    public void setProducerFactory(ProducerFactory producerFactory) {
        this.producerFactory = producerFactory;
    }

    public void setBufferManager(BufferManager bufferManger) {
        this.bufferManager = bufferManger;
    }

    private <R extends ConnectRecord<R>> List<Transformation<R>> getMysqlTransformations() {

        List<Transformation<R>> transformations = new ArrayList<>();
        Transformation<R> transformation = new Flatten.Value<R>();
        Map<String,String> map = new HashMap<>();
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
        HashMap<String, ? super Object> map = new HashMap<>();
        map.put(SCHEMA_ENABLE, false);
        keyConverter.configure(map, true);
        return keyConverter;

    }

    private Converter getValueConvert() {

        Converter valueConverter = new JsonConverter();
        HashMap<String, ? super Object> map = new HashMap<>();
        map.put(SCHEMA_ENABLE, false);
        map.put(JsonConverter.ROOT_NODE, schemaDescProperties.getOutputJsonNode());//schemaDescProperties.buildProperties());
        map.put(JsonConverterConfig.OPR_TYPE, JsonConverterConfig.OprType.OUTPUT);
        valueConverter.configure(map, false);
        return valueConverter;

    }

    private HeaderConverter getHeaderConvert() {

        HeaderConverter headerConvert = new JsonConverter();
//        HashMap<String, ? super Object> map = new HashMap<>();
//        map.put(SCHEMA_ENABLE, false);
//        headerConvert.configure(map);
        return headerConvert;

    }

    @Override
    public void setConsumerFactory(ConsumerFactory consumerFactory) {
        //do nothing
    }

    @Override
    public void setQueueManager(QueueManager queueManger) {

    }
}
