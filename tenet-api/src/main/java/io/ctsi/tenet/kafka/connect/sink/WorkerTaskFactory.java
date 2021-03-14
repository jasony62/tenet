package io.ctsi.tenet.kafka.connect.sink;



import io.ctsi.tenet.kafka.connect.metrics.ConnectMetrics;
import io.ctsi.tenet.kafka.connect.storage.MemoryStatusBackingStore;
import io.ctsi.tenet.kafka.factory.ConsumerFactory;
import io.ctsi.tenet.kafka.factory.ProducerFactory;
import io.ctsi.tenet.kafka.task.BufferManager;
import io.ctsi.tenet.kafka.task.QueueManager;
import org.apache.kafka.common.utils.Time;

public interface WorkerTaskFactory {

    /**
     * Create a consumer with the group id and client id as configured in the properties.
     * @return the consumer.
     */
    WorkerTask createTask();

    //void setSchemaDescConfig(SchemaDescProperties schemaDescProperties);

   // public void setTaskConfig(TaskConfig taskConfig);

    void setConsumerFactory(ConsumerFactory consumerFactory);

    void setProducerFactory(ProducerFactory producerFactory);

    void setQueueManager(QueueManager queueManger);

    void setBufferManager(BufferManager bufferManger);

    //void setTenetSinkProperties(TenetSinkProperties tenetSinkProperties);
    /*global var*/
    void setMemoryStatusBackingStore(MemoryStatusBackingStore memoryStatusBackingStore);
    /*global var*/
    void setTime(Time time);
    /*global var*/
    void setConnectMetrics(ConnectMetrics connectMetrics);
}
