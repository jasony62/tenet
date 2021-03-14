package io.ctsi.tenet.kafka.config;

import io.ctsi.tenet.kafka.connect.sink.SinkTask;
import io.ctsi.tenet.kafka.connect.sink.TaskConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.requests.IsolationLevel;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.PropertyMapper;
import org.springframework.util.CollectionUtils;
import org.springframework.util.unit.DataSize;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * 用于TENET相关配置，consumer配置，producer配置
 */
@ConfigurationProperties(prefix = "tenet.kafka")
public class TenetKafkaProperties {

    private final Map<String, String> properties = new HashMap();
    private final Consumer consumer = new Consumer();
    private final Producer producer = new Producer();

    public TenetKafkaProperties() {
    }

    public Map<String, String> getProperties() {
        return this.properties;
    }

    public Consumer getConsumer() {
        return this.consumer;
    }

    public Producer getProducer() {
        return this.producer;
    }

    private Map<String, Object> buildCommonProperties() {
        Map<String, Object> properties = new HashMap();

        if (!CollectionUtils.isEmpty(this.properties)) {
            properties.putAll(this.properties);
        }

        return properties;
    }

    public Map<String, Object> buildConsumerProperties() {
        Map<String, Object> properties = this.buildCommonProperties();
        properties.putAll(this.consumer.buildProperties());
        return properties;
    }

    public Map<String, Object> buildProducerProperties() {
        Map<String, Object> properties = this.buildCommonProperties();
        properties.putAll(this.producer.buildProperties());
        return properties;
    }

    private static class Properties extends HashMap<String, Object> {
        private Properties() {
        }

        <V> java.util.function.Consumer<V> in(String key) {
            return (value) -> {
                this.put(key, value);
            };
        }

        Properties with(Map<String, String> properties) {
            this.putAll(properties);
            return this;
        }
    }

    public static class Producer {
        private String acks;
        private DataSize batchSize;
        private List<String> bootstrapServers;
        private DataSize bufferMemory;
        private String clientId;
        private String compressionType;
        private Class<?> keySerializer = ByteArraySerializer.class;
        private Class<?> valueSerializer = ByteArraySerializer.class;
        private Integer retries;
        private String transactionIdPrefix;

        private final Map<String, String> properties = new HashMap();

        public Producer() {
        }

        public String getAcks() {
            return this.acks;
        }

        public void setAcks(String acks) {
            this.acks = acks;
        }

        public DataSize getBatchSize() {
            return this.batchSize;
        }

        public void setBatchSize(DataSize batchSize) {
            this.batchSize = batchSize;
        }

        public List<String> getBootstrapServers() {
            return this.bootstrapServers;
        }

        public void setBootstrapServers(List<String> bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
        }

        public DataSize getBufferMemory() {
            return this.bufferMemory;
        }

        public void setBufferMemory(DataSize bufferMemory) {
            this.bufferMemory = bufferMemory;
        }

        public String getClientId() {
            return this.clientId;
        }

        public void setClientId(String clientId) {
            this.clientId = clientId;
        }

        public String getCompressionType() {
            return this.compressionType;
        }

        public void setCompressionType(String compressionType) {
            this.compressionType = compressionType;
        }

        public Class<?> getKeySerializer() {
            return this.keySerializer;
        }

        public void setKeySerializer(Class<?> keySerializer) {
            this.keySerializer = keySerializer;
        }

        public Class<?> getValueSerializer() {
            return this.valueSerializer;
        }

        public void setValueSerializer(Class<?> valueSerializer) {
            this.valueSerializer = valueSerializer;
        }

        public Integer getRetries() {
            return this.retries;
        }

        public void setRetries(Integer retries) {
            this.retries = retries;
        }

        public String getTransactionIdPrefix() {
            return this.transactionIdPrefix;
        }

        public void setTransactionIdPrefix(String transactionIdPrefix) {
            this.transactionIdPrefix = transactionIdPrefix;
        }

        public Map<String, String> getProperties() {
            return this.properties;
        }

        public Map<String, Object> buildProperties() {
            Properties properties = new Properties();
            PropertyMapper map = PropertyMapper.get().alwaysApplyingWhenNonNull();
            map.from(this::getAcks).to(properties.in("acks"));
            map.from(this::getBatchSize).asInt(DataSize::toBytes).to(properties.in("batch.size"));
            map.from(this::getBootstrapServers).to(properties.in("bootstrap.servers"));
            map.from(this::getBufferMemory).as(DataSize::toBytes).to(properties.in("buffer.memory"));
            map.from(this::getClientId).to(properties.in("client.id"));
            map.from(this::getCompressionType).to(properties.in("compression.type"));
            map.from(this::getKeySerializer).to(properties.in("key.serializer"));
            map.from(this::getRetries).to(properties.in("retries"));
            map.from(this::getValueSerializer).to(properties.in("value.serializer"));
            return properties.with(this.properties);
        }
    }

    public static class Consumer {
        private Duration autoCommitInterval;
        private String autoOffsetReset;
        private List<String> bootstrapServers;
        private String clientId;
        private Boolean enableAutoCommit;
        private Duration fetchMaxWait;
        private DataSize fetchMinSize;
        private String groupId;
        private Duration heartbeatInterval;
        private IsolationLevel isolationLevel;
        private Class<?> keyDeserializer;
        private Class<?> valueDeserializer;
        private Integer maxPollRecords;
        private String offsetFlush;
        private Duration maxPollIntervalMs;
        private final Map<String, String> properties;

        public Consumer() {
            this.isolationLevel = IsolationLevel.READ_UNCOMMITTED;
            this.keyDeserializer = StringDeserializer.class;
            this.valueDeserializer = StringDeserializer.class;
            this.properties = new HashMap();
        }

        public Duration getAutoCommitInterval() {
            return this.autoCommitInterval;
        }

        public void setAutoCommitInterval(Duration autoCommitInterval) {
            this.autoCommitInterval = autoCommitInterval;
        }

        public String getAutoOffsetReset() {
            return this.autoOffsetReset;
        }

        public void setAutoOffsetReset(String autoOffsetReset) {
            this.autoOffsetReset = autoOffsetReset;
        }

        public List<String> getBootstrapServers() {
            return this.bootstrapServers;
        }

        public void setBootstrapServers(List<String> bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
        }

        public String getClientId() {
            return this.clientId;
        }

        public void setClientId(String clientId) {
            this.clientId = clientId;
        }

        public Boolean getEnableAutoCommit() {
            return this.enableAutoCommit;
        }

        public void setEnableAutoCommit(Boolean enableAutoCommit) {
            this.enableAutoCommit = enableAutoCommit;
        }

        public Duration getFetchMaxWait() {
            return this.fetchMaxWait;
        }

        public void setFetchMaxWait(Duration fetchMaxWait) {
            this.fetchMaxWait = fetchMaxWait;
        }

        public DataSize getFetchMinSize() {
            return this.fetchMinSize;
        }

        public void setFetchMinSize(DataSize fetchMinSize) {
            this.fetchMinSize = fetchMinSize;
        }

        public String getGroupId() {
            return this.groupId;
        }

        public void setGroupId(String groupId) {
            this.groupId = groupId;
        }

        public Duration getHeartbeatInterval() {
            return this.heartbeatInterval;
        }

        public void setHeartbeatInterval(Duration heartbeatInterval) {
            this.heartbeatInterval = heartbeatInterval;
        }

        public IsolationLevel getIsolationLevel() {
            return this.isolationLevel;
        }

        public void setIsolationLevel(IsolationLevel isolationLevel) {
            this.isolationLevel = isolationLevel;
        }

        public Class<?> getKeyDeserializer() {
            return this.keyDeserializer;
        }

        public void setKeyDeserializer(Class<?> keyDeserializer) {
            this.keyDeserializer = keyDeserializer;
        }

        public Class<?> getValueDeserializer() {
            return this.valueDeserializer;
        }

        public void setValueDeserializer(Class<?> valueDeserializer) {
            this.valueDeserializer = valueDeserializer;
        }

        public Integer getMaxPollRecords() {
            return this.maxPollRecords;
        }

        public void setMaxPollRecords(Integer maxPollRecords) {
            this.maxPollRecords = maxPollRecords;
        }

        public Map<String, String> getProperties() {
            return this.properties;
        }

        public String getOffsetFlush() {
            return offsetFlush;
        }

        public void setOffsetFlush(String offsetFlush) {
            this.offsetFlush = offsetFlush;
        }

        public Duration getMaxPollIntervalMs() {
            return maxPollIntervalMs;
        }

        public void setMaxPollIntervalMs(Duration maxPollIntervalMs) {
            this.maxPollIntervalMs = maxPollIntervalMs;
        }

        public Map<String, Object> buildProperties() {
            Properties properties = new Properties();
            PropertyMapper map = PropertyMapper.get().alwaysApplyingWhenNonNull();
            map.from(this::getAutoCommitInterval).asInt(Duration::toMillis).to(properties.in("auto.commit.interval.ms"));
            map.from(this::getAutoOffsetReset).to(properties.in("auto.offset.reset"));
            map.from(this::getBootstrapServers).to(properties.in("bootstrap.servers"));
            map.from(this::getClientId).to(properties.in("client.id"));
            map.from(this::getEnableAutoCommit).to(properties.in("enable.auto.commit"));
            map.from(this::getFetchMaxWait).asInt(Duration::toMillis).to(properties.in("fetch.max.wait.ms"));
            map.from(this::getFetchMinSize).asInt(DataSize::toBytes).to(properties.in("fetch.min.bytes"));
            map.from(this::getGroupId).to(properties.in("group.id"));
            map.from(this::getHeartbeatInterval).asInt(Duration::toMillis).to(properties.in("heartbeat.interval.ms"));
            map.from(() -> {
                return this.getIsolationLevel().name().toLowerCase(Locale.ROOT);
            }).to(properties.in("isolation.level"));
            map.from(this::getKeyDeserializer).to(properties.in("key.deserializer"));
            map.from(this::getValueDeserializer).to(properties.in("value.deserializer"));
            map.from(this::getMaxPollRecords).to(properties.in("max.poll.records"));
            map.from(this::getMaxPollIntervalMs).asInt(Duration::toMillis).to(properties.in("max.poll.interval.ms"));
            map.from(this::getOffsetFlush).to(properties.in(TaskConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG));
            return properties.with(this.properties);
        }
    }


    /**
     * Throw an exception if the passed-in properties do not constitute a valid sink.
     *
     * @param props sink configuration properties
     */
    public static void validate(Map<String, String> props) {
        final boolean hasTopicsConfig = hasTopicsConfig(props);
        final boolean hasTopicsRegexConfig = hasTopicsRegexConfig(props);

        if (hasTopicsConfig && hasTopicsRegexConfig) {
            throw new ConfigException(SinkTask.TOPICS_CONFIG + " and " + SinkTask.TOPICS_REGEX_CONFIG +
                    " are mutually exclusive options, but both are set.");
        }

        if (!hasTopicsConfig && !hasTopicsRegexConfig) {
            throw new ConfigException("Must configure one of " +
                    SinkTask.TOPICS_CONFIG + " or " + SinkTask.TOPICS_REGEX_CONFIG);
        }
    }

    public static final String TOPICS_CONFIG = SinkTask.TOPICS_CONFIG;
    public static final String TOPICS_REGEX_CONFIG = SinkTask.TOPICS_REGEX_CONFIG;

    public static boolean hasTopicsConfig(Map<String, String> props) {
        String topicsStr = props.get(TOPICS_CONFIG);
        return topicsStr != null && !topicsStr.trim().isEmpty();
    }

    public static boolean hasTopicsRegexConfig(Map<String, String> props) {
        String topicsRegexStr = props.get(TOPICS_REGEX_CONFIG);
        return topicsRegexStr != null && !topicsRegexStr.trim().isEmpty();
    }


}
