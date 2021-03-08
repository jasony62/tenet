package io.ctsi.tenet.framework.config;

import io.ctsi.tenet.framework.*;
import io.ctsi.tenet.framework.worker.DaemonTaskManager;
import io.ctsi.tenet.framework.worker.TenetThreadPool;
import io.ctsi.tenet.framework.worker.TenetWorkerLifecycle;
import io.ctsi.tenet.kafka.connect.storage.MemoryStatusBackingStore;
import io.ctsi.tenet.kafka.task.TenetWorkerTaskContainer;
import io.ctsi.tenet.kafka.task.BufferManager;
import io.ctsi.tenet.kafka.task.QueueManager;

import io.ctsi.tenet.kafka.task.util.TenetWorkerTaskConfigUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.utils.KafkaThread;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Mc.D
 */
@Configuration
@Slf4j
@EnableConfigurationProperties({TenetNodeScheduleConfig.class, TenetNodeTaskConfig.class})
public class TenetNodeAutoConfig {

    @Autowired(required = false)
    TenetTaskCache cache;

    @Bean
    @ConditionalOnMissingBean(TenetNode.class)
    public TenetNode tenetNode() {
        log.warn("No TenetNode bean found, default bean has been init, please check the code!");
        return new PreProcessNode();
    }

    @Bean
    @ConditionalOnMissingBean(TenetThreadPool.class)
    public TenetThreadPool tenetThreadPool(TenetNodeScheduleConfig tenetNodeScheduleConfig,
                                           TenetNodeTaskConfig taskConfig,
                                           TenetNode tenetNode,
                                           QueueManager queueManager) {
        log.warn("No TenetNode bean found, default bean has been init, please check the code!");
        return new TenetThreadPool(tenetNodeScheduleConfig.getDefaultThread(),
                tenetNodeScheduleConfig.getMaxThread(),
                60,
                TimeUnit.SECONDS,
                new ThreadFactory() {
                    private AtomicInteger count = new AtomicInteger(0);
                    public Thread newThread(Runnable runnable) {
                        return KafkaThread.nonDaemon("tenet-core-t"+count.getAndIncrement(), runnable);
                    }
                },
                queueManager,
                taskConfig,
                tenetNode);
    }

    @Bean
    public DaemonTaskManager daemonTaskManager(){
        return new DaemonTaskManager();
    }

    @Bean(name=TenetWorkerTaskConfigUtil.DEFAULT_TENET_MESSAGE_QUEUE_BEAN_NAME)
    public QueueManager queueManager(){
        return new QueueManager();
    }

    @Bean(name=TenetWorkerTaskConfigUtil.DEFAULT_TENET_MESSAGE_BUFFER_BEAN_NAME)
    public BufferManager bufferManagerManager(){
        return new BufferManager();
    }

    @Bean(name=TenetWorkerTaskConfigUtil.DEFAULT_MEMORY_STATUS_BACKING_STORE_BEAN_NAME)
    public MemoryStatusBackingStore memoryStatusBackingStore(){
        return new MemoryStatusBackingStore();
    }

    @Bean(name=TenetWorkerTaskConfigUtil.DEFAULT_TENET_WORKER_TASK_CONTAINER)
    public TenetWorkerTaskContainer workerTaskContainer(){
        return new TenetWorkerTaskContainer();
    }

    @Bean(name=TenetWorkerTaskConfigUtil.DEFAULT_TENET_MESSAGE_TASK_MANAGER_BEAN_NAME)
    public TenetWorkerLifecycle tenetWorkerLifecycle(){
        return new TenetWorkerLifecycle();
    }


    @AutoConfigureAfter(PreProcessConfig.class)
    class DefaultTenetNodeConfig {

        /*@Bean
        @ConditionalOnMissingBean
        @ConditionalOnProperty(name = "tenet.async.cache", havingValue = "redis")
        public TenetTaskCache tenetTaskCacheRedis(RedisTemplate<String, String> rt) {
            log.trace("init redis task cache");
            TenetTaskCache tenetTaskCache = new RedisTenetTaskCache(rt);
            if (cache == null) {
                cache = tenetTaskCache;
            }
            return tenetTaskCache;
        }

        @Bean
        @ConditionalOnMissingBean(TenetTaskCache.class)
        public TenetTaskCache tenetTaskCacheMap() {
            log.trace("init map task cache");
            TenetTaskCache tenetTaskCache = new MapTenetTaskCache();
            if (cache == null) {
                cache = tenetTaskCache;
            }
            return tenetTaskCache;
        }*/

        /*@ConditionalOnMissingBean(TenetNodeTaskHolder.class)
        @Bean
        @Autowired
        public TenetNodeTaskHolder tenetMessageTenetNodeTaskHolder(TenetNodeTaskConfig taskConfig, TenetNode tenetNode) {
            log.trace("init tenet task holder!");
            return new TenetNodeTaskHolder(taskConfig, tenetNode);
        }

        @Bean
        @ConditionalOnMissingBean
        @Autowired
        public TenetNodeSchedule tenetNodeSchedule(TenetNodeTaskConfig taskConfig, TenetNodeScheduleConfig config, TenetNodeTaskHolder taskHolder) {
            log.trace("init tenet schedule");
            return new TenetNodeSchedule(config, taskConfig, taskHolder);
        }*/
    }


    @ConditionalOnProperty(value = "tenet.pre.enabled", havingValue = "true")
    static class PreProcessConfig {
        @Bean
        @ConditionalOnMissingBean
        public TenetNode tenetNode() {
            log.debug("init tenet pre-process-node...");
            return new PreProcessNode();
        }
    }
}
