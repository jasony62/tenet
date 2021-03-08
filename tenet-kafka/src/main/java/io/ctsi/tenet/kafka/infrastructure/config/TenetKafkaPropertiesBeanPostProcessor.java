package io.ctsi.tenet.kafka.infrastructure.config;

import io.ctsi.tenet.kafka.config.TenetKafkaProperties;
import io.ctsi.tenet.kafka.task.TenetWorkerTaskContainer;
import io.ctsi.tenet.kafka.task.util.TenetWorkerTaskConfigUtil;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.*;
import org.springframework.core.Ordered;
import org.springframework.core.log.LogAccessor;
import org.springframework.util.Assert;

/**
 * 解析配置文件,为下一步初始化配置类作准备
 * 实现SmartInitializingSingleton
 * 初始化Registrar 的配置属性，方便初始化Registrar初始化bean
 */
public class TenetKafkaPropertiesBeanPostProcessor implements Ordered, BeanFactoryAware, SmartInitializingSingleton {

    public static final String TENET_PROPERTIES = "tenet.kafka-io.ctsi.tenet.kafka.config.TenetKafkaProperties";
    public static final String TENET_SCHEMA_PROPERTIES = "tenet.schema-io.ctsi.tenet.kafka.config.SchemaDescProperties";
    public static final String TENET_SINK_PROPERTIES = "tenet.sink-io.ctsi.tenet.kafka.config.TenetSinkProperties";

    private BeanFactory beanFactory;

    private final LogAccessor logger = new LogAccessor(LogFactory.getLog(getClass()));

    private final TenetWorkerTaskManagerRegistrar registrar = new TenetWorkerTaskManagerRegistrar();

    private TenetWorkerTaskContainer tenetWorkerTaskContainer;

    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        this.beanFactory = beanFactory;
    }

    @Override
    public void afterSingletonsInstantiated() throws BeansException {
        logger.debug("tenet afterSingletonsInstantiated init config");
        registrar.setBeanFactory(this.beanFactory);
        if (this.registrar.getTenetMessageTaskManager() == null) {
            if (this.tenetWorkerTaskContainer == null) {
                Assert.state(this.beanFactory != null,
                        "BeanFactory must be set to find tenetMessageTaskContainer by bean name");
                this.tenetWorkerTaskContainer = this.beanFactory.getBean(
                        TenetWorkerTaskConfigUtil.DEFAULT_TENET_WORKER_TASK_CONTAINER, TenetWorkerTaskContainer.class);
            }
            this.registrar.setTenetMessageTaskManager(this.tenetWorkerTaskContainer);
        }

        TenetKafkaProperties tenetKafkaProperties = null;

        Assert.state(this.beanFactory != null, "BeanFactory must be set to obtain container factory by bean name");
        try {
            tenetKafkaProperties = this.beanFactory.getBean(TENET_PROPERTIES, TenetKafkaProperties.class);
            //tenetKafkaProperties = this.applicationContext.getBean(TENET_PROPERTIES, TenetKafkaProperties.class);
        } catch (NoSuchBeanDefinitionException ex) {
            throw new BeanInitializationException("Could not register TenetKafkaProperties  no " + TenetKafkaProperties.class.getSimpleName() + " was found in the application context", ex);
        }

        this.registrar.registerTaskProperties(tenetKafkaProperties);
        registrar.setWorkerTaskFactoryBeanName(TenetWorkerTaskConfigUtil.DEFAULT_WORKER_TASK_FACTORY_BEAN_NAME);
        registrar.setProducerTaskFactoryBeanName(TenetWorkerTaskConfigUtil.DEFAULT_PRODUCER_TASK_FACTORY_BEAN_NAME);
        registrar.configure();

    }

    @Override
    public int getOrder() {
        return LOWEST_PRECEDENCE;
    }
}
