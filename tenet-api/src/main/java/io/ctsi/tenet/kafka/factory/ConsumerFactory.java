package io.ctsi.tenet.kafka.factory;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.lang.Nullable;

import java.util.Map;
import java.util.Properties;

public interface ConsumerFactory <K, V> {

    /**
     * Create a consumer with the group id and client id as configured in the properties.
     * @return the consumer.
     */
    default KafkaConsumer<K, V> createConsumer() {
        return createConsumer(null);
    }

    /**
     * Create a consumer, appending the suffix to the {@code client.id} property,
     * if present.
     * @param clientIdSuffix the suffix.
     * @return the consumer.
     * @since 1.3
     */
    default KafkaConsumer<K, V> createConsumer(@Nullable String clientIdSuffix) {
        return createConsumer(null, clientIdSuffix);
    }

    /**
     * Create a consumer with an explicit group id; in addition, the
     * client id suffix is appended to the {@code client.id} property, if both
     * are present.
     * @param groupId the group id.
     * @param clientIdSuffix the suffix.
     * @return the consumer.
     * @since 1.3
     */
    default KafkaConsumer<K, V> createConsumer(@Nullable String groupId, @Nullable String clientIdSuffix) {
        return createConsumer(groupId, null, clientIdSuffix);
    }

    /**
     * Create a consumer with an explicit group id; in addition, the
     * client id suffix is appended to the clientIdPrefix which overrides the
     * {@code client.id} property, if present.
     * @param groupId the group id.
     * @param clientIdPrefix the prefix.
     * @param clientIdSuffix the suffix.
     * @return the consumer.
     * @since 2.1.1
     */
    KafkaConsumer<K, V> createConsumer(@Nullable String groupId, @Nullable String clientIdPrefix,
                                  @Nullable String clientIdSuffix);

    /**
     * Create a consumer with an explicit group id; in addition, the
     * client id suffix is appended to the clientIdPrefix which overrides the
     * {@code client.id} property, if present. In addition, consumer properties can
     * be overridden if the factory implementation supports it.
     * @param groupId the group id.
     * @param clientIdPrefix the prefix.
     * @param clientIdSuffix the suffix.
     * @param properties the properties to override.
     * @return the consumer.
     * @since 2.2.4
     */
    default Consumer<K, V> createConsumer(@Nullable String groupId, @Nullable String clientIdPrefix,
                                          @Nullable String clientIdSuffix, @Nullable Properties properties) {

        return createConsumer(groupId, clientIdPrefix, clientIdSuffix);
    }

    /**
     * Return true if consumers created by this factory use auto commit.
     * @return true if auto commit.
     */
    boolean isAutoCommit();

    /**
     * Return an unmodifiable reference to the configuration map for this factory.
     * Useful for cloning to make a similar factory.
     * @return the configs.
     * @since 2.0
     */
    default Map<String, Object> getConfigurationProperties() {
        throw new UnsupportedOperationException("'getConfigurationProperties()' is not supported");
    }

    /**
     * Return the configured key deserializer (if provided as an object instead
     * of a class name in the properties).
     * @return the deserializer.
     * @since 2.0
     */
    @Nullable
    default Deserializer<K> getKeyDeserializer() {
        return null;
    }

    /**
     * Return the configured value deserializer (if provided as an object instead
     * of a class name in the properties).
     * @return the deserializer.
     * @since 2.0
     */
    @Nullable
    default Deserializer<V> getValueDeserializer() {
        return null;
    }

}
