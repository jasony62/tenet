package io.ctsi.tenet.kafka.connect.transforms;

import io.ctsi.tenet.kafka.connect.ConnectRecord;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.ConfigDef;

import java.io.Closeable;

public interface Transformation<R extends ConnectRecord<R>> extends Configurable, Closeable {

    /**
     * Apply transformation to the {@code record} and return another record object (which may be {@code record} itself) or {@code null},
     * corresponding to a map or filter operation respectively.
     * <p>
     * The implementation must be thread-safe.
     */
    R apply(R record);

    /**
     * Configuration specification for this transformation.
     **/
    ConfigDef config();

    /**
     * Signal that this transformation instance will no longer will be used.
     **/
    @Override
    void close();

}
