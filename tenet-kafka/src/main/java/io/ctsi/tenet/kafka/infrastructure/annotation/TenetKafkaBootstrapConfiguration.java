package io.ctsi.tenet.kafka.infrastructure.annotation;

import io.ctsi.tenet.kafka.connect.storage.MemoryStatusBackingStore;
import io.ctsi.tenet.kafka.infrastructure.config.TenetKafkaPropertiesBeanPostProcessor;
import io.ctsi.tenet.kafka.infrastructure.core.TenetProducerTaskFactory;
import io.ctsi.tenet.kafka.infrastructure.core.TenetWorkerTaskFactory;
import io.ctsi.tenet.kafka.task.util.TenetWorkerTaskConfigUtil;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.type.AnnotationMetadata;

/**
 * 主要解决扫描包问题，通过springboot提前getRunListeners spi机制手动注册项目的bean
 */
public class TenetKafkaBootstrapConfiguration implements ImportBeanDefinitionRegistrar {

    @Override
    public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry) {
        if (!registry.containsBeanDefinition(
                TenetWorkerTaskConfigUtil.TENET_MESSAGE_PROPERTIES_PROCESSOR_BEAN_NAME)) {

            registry.registerBeanDefinition(TenetWorkerTaskConfigUtil.TENET_MESSAGE_PROPERTIES_PROCESSOR_BEAN_NAME,
                    new RootBeanDefinition(TenetKafkaPropertiesBeanPostProcessor.class));
        }

//        if (!registry.containsBeanDefinition(TenetWorkerTaskConfigUtil.TENET_MESSAGE_TASK_MANAGER_BEAN_NAME)) {
//            registry.registerBeanDefinition(TenetWorkerTaskConfigUtil.TENET_MESSAGE_TASK_MANAGER_BEAN_NAME,
//                    new RootBeanDefinition(TenetWorkerTaskManager.class));
//        }

//        if (!registry.containsBeanDefinition(TenetWorkerTaskConfigUtil.TENET_MESSAGE_QUEUE_BEAN_NAME)) {
//            registry.registerBeanDefinition(TenetWorkerTaskConfigUtil.TENET_MESSAGE_QUEUE_BEAN_NAME,
//                    new RootBeanDefinition(QueueManager.class));
//        }

//        if (!registry.containsBeanDefinition(TenetWorkerTaskConfigUtil.TENET_MESSAGE_BUFFER_BEAN_NAME)) {
//            registry.registerBeanDefinition(TenetWorkerTaskConfigUtil.TENET_MESSAGE_BUFFER_BEAN_NAME,
//                    new RootBeanDefinition(BufferManager.class));
//        }

        if (!registry.containsBeanDefinition(TenetWorkerTaskConfigUtil.DEFAULT_WORKER_TASK_FACTORY_BEAN_NAME)) {
            registry.registerBeanDefinition(TenetWorkerTaskConfigUtil.DEFAULT_WORKER_TASK_FACTORY_BEAN_NAME,
                    new RootBeanDefinition(TenetWorkerTaskFactory.class));
        }

        if (!registry.containsBeanDefinition(TenetWorkerTaskConfigUtil.DEFAULT_PRODUCER_TASK_FACTORY_BEAN_NAME)) {
            registry.registerBeanDefinition(TenetWorkerTaskConfigUtil.DEFAULT_PRODUCER_TASK_FACTORY_BEAN_NAME,
                    new RootBeanDefinition(TenetProducerTaskFactory.class));
        }
        
//        if (!registry.containsBeanDefinition(TenetWorkerTaskConfigUtil.DEFAULT_MEMORY_STATUS_BACKING_STORE_BEAN_NAME)) {
//            registry.registerBeanDefinition(TenetWorkerTaskConfigUtil.DEFAULT_MEMORY_STATUS_BACKING_STORE_BEAN_NAME,
//                    new RootBeanDefinition(MemoryStatusBackingStore.class));
//        }
    }

}