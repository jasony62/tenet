package io.ctsi.tenet.kafka.connect.storage;

import io.ctsi.tenet.kafka.connect.util.Callback;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Future;

public interface OffsetBackingStore {

    /**
     * Start this offset store.
     */
    void start();

    /**
     * Stop the backing store. Implementations should attempt to shutdown gracefully, but not block
     * indefinitely.
     */
    void stop();

    /**
     * Get the values for the specified keys
     * @param keys list of keys to look up
     * @return future for the resulting map from key to value
     */
    Future<Map<ByteBuffer, ByteBuffer>> get(Collection<ByteBuffer> keys);

    /**
     * Set the specified keys and values.
     * @param values map from key to value
     * @param callback callback to invoke on completion
     * @return void future for the operation
     */
    Future<Void> set(Map<ByteBuffer, ByteBuffer> values, Callback<Void> callback);

    /**
     * Configure class with the given key-value pairs
     * @param config can be DistributedConfig or StandaloneConfig
     */
    //void configure(WorkerConfig config);
}
