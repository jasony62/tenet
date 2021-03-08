package io.ctsi.tenet.kafka.infrastructure.config;

import io.ctsi.tenet.kafka.config.SchemaDescProperties;
import io.ctsi.tenet.kafka.config.TenetKafkaProperties;
import io.ctsi.tenet.kafka.config.TenetSinkProperties;
import io.ctsi.tenet.kafka.connect.metrics.ConnectMetrics;
import io.ctsi.tenet.kafka.connect.storage.MemoryStatusBackingStore;
import io.ctsi.tenet.kafka.factory.ConsumerFactory;
import io.ctsi.tenet.kafka.factory.ProducerFactory;
import io.ctsi.tenet.kafka.infrastructure.core.TenetConsumerFactory;
import io.ctsi.tenet.kafka.infrastructure.core.TenetProducerFactory;
import io.ctsi.tenet.kafka.infrastructure.core.TenetProducerTaskFactory;
import io.ctsi.tenet.kafka.infrastructure.core.TenetWorkerTaskFactory;
import io.ctsi.tenet.kafka.task.TenetWorkerTaskContainer;
import io.ctsi.tenet.kafka.task.BufferManager;
import io.ctsi.tenet.kafka.task.QueueManager;
import io.ctsi.tenet.kafka.task.util.TenetWorkerTaskConfigUtil;
import org.apache.kafka.common.utils.Time;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;

/**
 * 管理Task配置
 * 配置TenetManager
 */
public class TenetWorkerTaskManagerRegistrar {

    private BeanFactory beanFactory;

    private String workerTaskFactoryBeanName;

    private String producerTaskFactoryBeanName;

    private TenetWorkerTaskContainer tenetWorkerTaskContainer;

    private List<TenetKafkaProperties> taskList = new ArrayList<>();

    private TenetWorkerTaskFactory tenetWorkerTaskFactory;

    private TenetProducerTaskFactory tenetProducerTaskFactory;


    public void setBeanFactory(BeanFactory beanFactory) {
        this.beanFactory = beanFactory;
    }

    public void setWorkerTaskFactoryBeanName(String workerTaskFactoryBeanName) {
        this.workerTaskFactoryBeanName = workerTaskFactoryBeanName;
    }

    public void setProducerTaskFactoryBeanName(String producerTaskFactoryBeanName) {
        this.producerTaskFactoryBeanName = producerTaskFactoryBeanName;
    }

    /**
     * 此方法在所有bean加载后执行
     * 需要获取tenet-api
     * QueueManager
     * MemoryStatusBackingStore
     * BufferManager
     * Bean 单例
     */
    public void configure() {

        TenetWorkerTaskFactory tenetWorkerTaskFactory = resolveWorkerFactory();
        TenetProducerTaskFactory tenetProducerTaskFactory = resolveProducerFactory();
        SchemaDescProperties schemaDescProperties = null;
        TenetSinkProperties tenetSinkProperties = null;
        ConsumerFactory consumerFactory = null;
        ProducerFactory producerFactory = null;
        QueueManager queueManager = null;
        BufferManager bufferManager = null;
        MemoryStatusBackingStore memoryStatusBackingStore = null;

        Assert.state(this.beanFactory != null, "BeanFactory must be set to obtain container factory by bean name");
        try {
            schemaDescProperties = this.beanFactory.getBean(TenetKafkaPropertiesBeanPostProcessor.TENET_SCHEMA_PROPERTIES, SchemaDescProperties.class);
        } catch (NoSuchBeanDefinitionException ex) {
            throw new BeanInitializationException("Could not register TenetKafkaProperties  no " + SchemaDescProperties.class.getSimpleName() + " was found in the application context", ex);
        }
        try {
            tenetSinkProperties = this.beanFactory.getBean(TenetKafkaPropertiesBeanPostProcessor.TENET_SINK_PROPERTIES, TenetSinkProperties.class);
        } catch (NoSuchBeanDefinitionException ex) {
            throw new BeanInitializationException("Could not register TenetSinkProperties  no " + SchemaDescProperties.class.getSimpleName() + " was found in the application context", ex);
        }

        try {
            queueManager = this.beanFactory.getBean(TenetWorkerTaskConfigUtil.DEFAULT_TENET_MESSAGE_QUEUE_BEAN_NAME, QueueManager.class);
        } catch (NoSuchBeanDefinitionException ex) {
            throw new BeanInitializationException("Could not register queueManager ", ex);
        }

        try {
            memoryStatusBackingStore = this.beanFactory.getBean(TenetWorkerTaskConfigUtil.DEFAULT_MEMORY_STATUS_BACKING_STORE_BEAN_NAME,MemoryStatusBackingStore.class);
        } catch (NoSuchBeanDefinitionException ex) {
            throw new BeanInitializationException("Could not register memoryStatusBackingStore ", ex);
        }

        try {
            bufferManager = this.beanFactory.getBean(TenetWorkerTaskConfigUtil.DEFAULT_TENET_MESSAGE_BUFFER_BEAN_NAME, BufferManager.class);
        } catch (NoSuchBeanDefinitionException ex) {
            throw new BeanInitializationException("Could not register bufferManager ", ex);
        }

        for (TenetKafkaProperties task : this.taskList) {

            Time time = Time.SYSTEM;
            ConnectMetrics connectMetrics = new ConnectMetrics(TenetWorkerTaskConfigUtil.GENERATED_METRICS_PREFIX, tenetSinkProperties.buildProperties(), time);

            consumerFactory = new TenetConsumerFactory(task.buildConsumerProperties());
            tenetWorkerTaskFactory.setSchemaDescConfig(schemaDescProperties);
            tenetWorkerTaskFactory.setConsumerFactory(consumerFactory);
            tenetWorkerTaskFactory.setQueueManager(queueManager);
            tenetWorkerTaskFactory.setTenetSinkProperties(tenetSinkProperties);
            tenetWorkerTaskFactory.setConnectMetrics(connectMetrics);
            tenetWorkerTaskFactory.setMemoryStatusBackingStore(memoryStatusBackingStore);
            tenetWorkerTaskFactory.setTime(time);

            producerFactory = new TenetProducerFactory(task.buildProducerProperties());
            tenetProducerTaskFactory.setSchemaDescConfig(schemaDescProperties);
            tenetProducerTaskFactory.setProducerFactory(producerFactory);
            tenetProducerTaskFactory.setBufferManager(bufferManager);
            tenetProducerTaskFactory.setTenetSinkProperties(tenetSinkProperties);
            tenetProducerTaskFactory.setMemoryStatusBackingStore(memoryStatusBackingStore);
            tenetProducerTaskFactory.setConnectMetrics(connectMetrics);
            tenetProducerTaskFactory.setTime(time);

            /*注意顺序先启动*/
            this.tenetWorkerTaskContainer.setBuildProperties(tenetSinkProperties.buildProperties());
            this.tenetWorkerTaskContainer.registerTenetMessageTask(tenetProducerTaskFactory.createTask());
            this.tenetWorkerTaskContainer.registerTenetMessageTask(tenetWorkerTaskFactory.createTask());
            this.tenetWorkerTaskContainer.setConnectMetricsAndWorkerMetricsGroup(connectMetrics);

        }

        //}
    }

    private TenetWorkerTaskFactory resolveWorkerFactory() {
        if (this.tenetWorkerTaskFactory != null) {
            return this.tenetWorkerTaskFactory;
        }
        if (this.workerTaskFactoryBeanName != null) {
            Assert.state(this.beanFactory != null, "BeanFactory must be set to obtain container factory by bean name");
            this.tenetWorkerTaskFactory = this.beanFactory.getBean(
                    this.workerTaskFactoryBeanName, TenetWorkerTaskFactory.class);
            return this.tenetWorkerTaskFactory;  // Consider changing this if live change of the factory is required
        } else {
            throw new IllegalStateException("Could not resolve the " +
                    TenetWorkerTaskFactory.class.getSimpleName() + " to use for [" +
                    this.workerTaskFactoryBeanName + "] no factory was given and no default is set.");
        }
    }

    private TenetProducerTaskFactory resolveProducerFactory() {
        if (this.tenetProducerTaskFactory != null) {
            return this.tenetProducerTaskFactory;
        }
        if (this.producerTaskFactoryBeanName != null) {
            Assert.state(this.beanFactory != null, "BeanFactory must be set to obtain container factory by bean name");
            this.tenetProducerTaskFactory = this.beanFactory.getBean(
                    this.producerTaskFactoryBeanName, TenetProducerTaskFactory.class);
            return this.tenetProducerTaskFactory;  // Consider changing this if live change of the factory is required
        } else {
            throw new IllegalStateException("Could not resolve the " +
                    TenetWorkerTaskFactory.class.getSimpleName() + " to use for [" +
                    this.producerTaskFactoryBeanName + "] no factory was given and no default is set.");
        }
    }

    public TenetWorkerTaskContainer getTenetMessageTaskManager() {
        return tenetWorkerTaskContainer;
    }

    public void setTenetMessageTaskManager(TenetWorkerTaskContainer tenetMessageTaskManager) {
        this.tenetWorkerTaskContainer = tenetMessageTaskManager;
    }

    public void registerTaskProperties(TenetKafkaProperties properties) {
        Assert.notNull(properties, "TaskProperties must be set");
        //Assert.hasText(endpoint.getId(), "TaskProperties id must be set");
        // Factory may be null, we defer the resolution right before actually creating the container
        //KafkaListenerEndpointDescriptor descriptor = new KafkaListenerEndpointDescriptor(endpoint, factory);
        synchronized (this.taskList) {
            //
            this.taskList.add(properties);
        }
    }

}
