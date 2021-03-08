package io.ctsi.tenet.kafka.connect.storage;


import io.ctsi.tenet.kafka.connect.error.ConnectException;
import io.ctsi.tenet.kafka.connect.util.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

public class MemoryOffsetBackingStore  implements OffsetBackingStore {
    private static final Logger log = LoggerFactory.getLogger(MemoryOffsetBackingStore.class);

    protected Map<ByteBuffer, ByteBuffer> data = new HashMap<>();
    protected ExecutorService executor;

    public MemoryOffsetBackingStore() {

    }

//    @Override
//    public void configure(WorkerConfig config) {
//    }

    @Override
    public void start() {
        executor = Executors.newSingleThreadExecutor();
    }

    @Override
    public void stop() {
        if (executor != null) {
            executor.shutdown();
            // Best effort wait for any get() and set() tasks (and caller's callbacks) to complete.
            try {
                executor.awaitTermination(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (!executor.shutdownNow().isEmpty()) {
                throw new ConnectException("Failed to stop MemoryOffsetBackingStore. Exiting without cleanly " +
                        "shutting down pending tasks and/or callbacks.");
            }
            executor = null;
        }
    }

    @Override
    public Future<Map<ByteBuffer, ByteBuffer>> get(final Collection<ByteBuffer> keys) {
        return executor.submit(new Callable<Map<ByteBuffer, ByteBuffer>>() {
            @Override
            public Map<ByteBuffer, ByteBuffer> call() throws Exception {
                Map<ByteBuffer, ByteBuffer> result = new HashMap<>();
                for (ByteBuffer key : keys) {
                    result.put(key, data.get(key));
                }
                return result;
            }
        });

    }

    @Override
    public Future<Void> set(final Map<ByteBuffer, ByteBuffer> values,
                            final Callback<Void> callback) {
        return executor.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                for (Map.Entry<ByteBuffer, ByteBuffer> entry : values.entrySet()) {
                    data.put(entry.getKey(), entry.getValue());
                }
                save();
                if (callback != null)
                    callback.onCompletion(null, null);
                return null;
            }
        });
    }

    // Hook to allow subclasses to persist data
    protected void save() {

    }
}