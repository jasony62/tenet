package io.ctsi.tenet.kafka.infrastructure.core;

import io.ctsi.tenet.kafka.factory.ConsumerFactory;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.core.log.LogAccessor;
import org.springframework.lang.Nullable;
import org.springframework.util.StringUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;

public class TenetConsumerFactory<K, V> implements ConsumerFactory<K, V>, BeanNameAware {

    private static final LogAccessor LOGGER = new LogAccessor(LogFactory.getLog(TenetConsumerFactory.class));

    private final Map<String, Object> configs;

    private Supplier<Deserializer<K>> keyDeserializerSupplier;

    private Supplier<Deserializer<V>> valueDeserializerSupplier;

    /**
     * Construct a factory with the provided configuration.
     *
     * @param configs the configuration.
     */
    public TenetConsumerFactory(Map<String, Object> configs) {
        this(configs, () -> null, () -> null);
    }

    /**
     * Construct a factory with the provided configuration and deserializers.
     *
     * @param configs           the configuration.
     * @param keyDeserializer   the key {@link Deserializer}.
     * @param valueDeserializer the value {@link Deserializer}.
     */
    public TenetConsumerFactory(Map<String, Object> configs,
                                @Nullable Deserializer<K> keyDeserializer,
                                @Nullable Deserializer<V> valueDeserializer) {

        this(configs, () -> keyDeserializer, () -> valueDeserializer);
    }

    /**
     * Construct a factory with the provided configuration and deserializer suppliers.
     *
     * @param configs                   the configuration.
     * @param keyDeserializerSupplier   the key {@link Deserializer} supplier function.
     * @param valueDeserializerSupplier the value {@link Deserializer} supplier function.
     * @since 2.3
     */
    public TenetConsumerFactory(Map<String, Object> configs,
                                       @Nullable Supplier<Deserializer<K>> keyDeserializerSupplier,
                                       @Nullable Supplier<Deserializer<V>> valueDeserializerSupplier) {

        this.configs = new HashMap<>(configs);
        this.keyDeserializerSupplier = keyDeserializerSupplier == null ? () -> null : keyDeserializerSupplier;
        this.valueDeserializerSupplier = valueDeserializerSupplier == null ? () -> null : valueDeserializerSupplier;
    }

    @Override
    public void setBeanName(String name) {
    }

    public void setKeyDeserializer(@Nullable Deserializer<K> keyDeserializer) {
        this.keyDeserializerSupplier = () -> keyDeserializer;
    }

    public void setValueDeserializer(@Nullable Deserializer<V> valueDeserializer) {
        this.valueDeserializerSupplier = () -> valueDeserializer;
    }

    @Override
    public Map<String, Object> getConfigurationProperties() {
        return Collections.unmodifiableMap(this.configs);
    }

    @Override
    public Deserializer<K> getKeyDeserializer() {
        return this.keyDeserializerSupplier.get();
    }

    @Override
    public Deserializer<V> getValueDeserializer() {
        return this.valueDeserializerSupplier.get();
    }

    @Override
    public KafkaConsumer<K, V> createConsumer(@Nullable String groupId, @Nullable String clientIdPrefix,
                                         @Nullable String clientIdSuffix) {

        return createKafkaConsumer(groupId, clientIdPrefix, clientIdSuffix, null);
    }

    @Override
    public Consumer<K, V> createConsumer(@Nullable String groupId, @Nullable String clientIdPrefix,
                                         @Nullable final String clientIdSuffixArg, @Nullable Properties properties) {

        return createKafkaConsumer(groupId, clientIdPrefix, clientIdSuffixArg, properties);
    }

    @Deprecated
    protected KafkaConsumer<K, V> createKafkaConsumer(@Nullable String groupId, @Nullable String clientIdPrefix,
                                                      @Nullable String clientIdSuffixArg) {

        return createKafkaConsumer(groupId, clientIdPrefix, clientIdSuffixArg, null);
    }

    protected KafkaConsumer<K, V> createKafkaConsumer(@Nullable String groupId, @Nullable String clientIdPrefix,
                                                      @Nullable String clientIdSuffixArg, @Nullable Properties properties) {

        boolean overrideClientIdPrefix = StringUtils.hasText(clientIdPrefix);
        String clientIdSuffix = clientIdSuffixArg;
        if (clientIdSuffix == null) {
            clientIdSuffix = "";
        }
        boolean shouldModifyClientId = (this.configs.containsKey(ConsumerConfig.CLIENT_ID_CONFIG)
                && StringUtils.hasText(clientIdSuffix)) || overrideClientIdPrefix;
        if (groupId == null
                && (properties == null || properties.stringPropertyNames().size() == 0)
                && !shouldModifyClientId) {
            return createKafkaConsumer(this.configs);
        } else {
            return createConsumerWithAdjustedProperties(groupId, clientIdPrefix, properties, overrideClientIdPrefix,
                    clientIdSuffix, shouldModifyClientId);
        }
    }

    private KafkaConsumer<K, V> createConsumerWithAdjustedProperties(String groupId, String clientIdPrefix,
                                                                     Properties properties, boolean overrideClientIdPrefix, String clientIdSuffix,
                                                                     boolean shouldModifyClientId) {

        Map<String, Object> modifiedConfigs = new HashMap<>(this.configs);
        if (groupId != null) {
            modifiedConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        }
        if (shouldModifyClientId) {
            modifiedConfigs.put(ConsumerConfig.CLIENT_ID_CONFIG,
                    (overrideClientIdPrefix ? clientIdPrefix
                            : modifiedConfigs.get(ConsumerConfig.CLIENT_ID_CONFIG)) + clientIdSuffix);
        }
        if (properties != null) {
            checkForUnsupportedProps(properties);
            properties.stringPropertyNames()
                    .stream()
                    .filter(name -> !name.equals(ConsumerConfig.CLIENT_ID_CONFIG)
                            && !name.equals(ConsumerConfig.GROUP_ID_CONFIG))
                    .forEach(name -> modifiedConfigs.put(name, properties.getProperty(name)));
        }
        return createKafkaConsumer(modifiedConfigs);
    }

    private void checkForUnsupportedProps(Properties properties) {
        properties.forEach((key, value) -> {
            if (!(key instanceof String) || !(value instanceof String)) {
                LOGGER.warn(() -> "Property override for '" + key.toString()
                        + "' ignored, only <String, String> properties are supported; value is a(n) " + value.getClass());
            }
        });
    }

    protected KafkaConsumer<K, V> createKafkaConsumer(Map<String, Object> configProps) {

        return new KafkaConsumer<>(configProps, this.keyDeserializerSupplier.get(),
                this.valueDeserializerSupplier.get());
    }

    @Override
    public boolean isAutoCommit() {
        Object auto = this.configs.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
        return auto instanceof Boolean ? (Boolean) auto
                : auto instanceof String ? Boolean.valueOf((String) auto) : true;
    }

}

