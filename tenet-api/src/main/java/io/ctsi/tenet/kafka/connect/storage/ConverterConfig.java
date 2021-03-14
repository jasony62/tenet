package io.ctsi.tenet.kafka.connect.storage;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.ValidString.in;

public abstract  class ConverterConfig extends AbstractConfig {

    public static final String TYPE_CONFIG = "converter.type";
    private static final String TYPE_DOC = "How this converter will be used.";

    /**
     * Create a new {@link ConfigDef} instance containing the configurations defined by ConverterConfig. This can be called by subclasses.
     *
     * @return the ConfigDef; never null
     */
    public static ConfigDef newConfigDef() {
        return new ConfigDef().define(TYPE_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                in(io.ctsi.tenet.kafka.connect.storage.ConverterType.KEY.getName(), io.ctsi.tenet.kafka.connect.storage.ConverterType.VALUE.getName(), io.ctsi.tenet.kafka.connect.storage.ConverterType.HEADER.getName()),
                ConfigDef.Importance.LOW, TYPE_DOC);
    }

    protected ConverterConfig(ConfigDef configDef, Map<String, ?> props) {
        super(configDef, props, true);
    }

    /**
     * Get the type of converter as defined by the {@link #TYPE_CONFIG} configuration.
     * @return the converter type; never null
     */
    public io.ctsi.tenet.kafka.connect.storage.ConverterType type() {
        return io.ctsi.tenet.kafka.connect.storage.ConverterType.withName(getString(TYPE_CONFIG));
    }
}

