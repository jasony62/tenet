package io.ctsi.tenet.kafka.task.util;

public abstract class TenetWorkerTaskConfigUtil {

    /**
     * The bean name of the internally managed config processor.
     */
    public static final String TENET_MESSAGE_PROPERTIES_PROCESSOR_BEAN_NAME =
            "io.ctsi.tenet.kafka.infrastructure.config.internalTenetMessagePropertiesBeanPostProcessor";

    /**
     * The bean name of the internally managed  registry.
     */
    public static final String DEFAULT_TENET_MESSAGE_TASK_MANAGER_BEAN_NAME =
            "io.ctsi.tenet.kafka.infrastructure.config.internalWorkerLifecycle";

    /**
     * The bean name of the internally managed  registry.
     */
    public static final String DEFAULT_TENET_MESSAGE_QUEUE_BEAN_NAME =
            "io.ctsi.tenet.kafka.infrastructure.config.internalQueueManager";

    /**
     * The bean name of the internally managed  registry.
     */
    public static final String DEFAULT_TENET_MESSAGE_BUFFER_BEAN_NAME =
            "io.ctsi.tenet.kafka.infrastructure.config.internalBufferManager";

    /**
     * The bean name of the internally managed  registry.
     */
    public static final String DEFAULT_WORKER_TASK_FACTORY_BEAN_NAME =
            "io.ctsi.tenet.kafka.infrastructure.config.internalWorkerTaskFactory";

    /**
     * The bean name of the internally managed  registry.
     */
    public static final String DEFAULT_PRODUCER_TASK_FACTORY_BEAN_NAME =
            "io.ctsi.tenet.kafka.infrastructure.config.internalProducerTaskFactory";

    public static final String DEFAULT_MEMORY_STATUS_BACKING_STORE_BEAN_NAME =
            "io.ctsi.tenet.kafka.infrastructure.config.internalMemoryStatusBackingStore";

    public static final String GENERATED_ID_WORKER_PREFIX = "io.ctsi.kafka.TenetWorkerTask#";

    public static final String GENERATED_ID_PRODUCER_PREFIX = "io.ctsi.kafka.TenetProducerTask#";

    public static final String GENERATED_METRICS_PREFIX = "io.ctsi.kafka.TenetConnectMetrics";

    public static final String DEFAULT_TENET_WORKER_TASK_CONTAINER = "io.ctsi.tenet.kafka.task.TenetWorkerTaskContainer";

}
