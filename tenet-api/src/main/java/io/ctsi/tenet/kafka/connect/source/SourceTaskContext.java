package io.ctsi.tenet.kafka.connect.source;

import io.ctsi.tenet.kafka.connect.storage.OffsetStorageReader;

import java.util.Map;

public interface SourceTaskContext {
    /**
     * Get the Task configuration.  This is the latest configuration and may differ from that passed on startup.
     *
     * For example, this method can be used to obtain the latest configuration if an external secret has changed,
     * and the configuration is using variable references such as those compatible with
     * {@link org.apache.kafka.common.config.ConfigTransformer}.
     */
    public Map<String, String> configs();

    /**
     * Get the OffsetStorageReader for this SourceTask.
     */
    OffsetStorageReader offsetStorageReader();
}
