package io.ctsi.tenet.kafka.connect.storage;

import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class StringConverterConfig extends io.ctsi.tenet.kafka.connect.storage.ConverterConfig {

    public static final String ENCODING_CONFIG = "converter.encoding";
    public static final String ENCODING_DEFAULT = "UTF8";
    private static final String ENCODING_DOC = "The name of the Java character set to use for encoding strings as byte arrays.";
    private static final String ENCODING_DISPLAY = "Encoding";

    private final static ConfigDef CONFIG;

    static {
        CONFIG = newConfigDef();
        CONFIG.define(ENCODING_CONFIG, ConfigDef.Type.STRING, ENCODING_DEFAULT, ConfigDef.Importance.HIGH, ENCODING_DOC, null, -1, ConfigDef.Width.MEDIUM,
                ENCODING_DISPLAY);
    }

    public static ConfigDef configDef() {
        return CONFIG;
    }

    public StringConverterConfig(Map<String, ?> props) {
        super(CONFIG, props);
    }

    /**
     * Get the string encoding.
     *
     * @return the encoding; never null
     */
    public String encoding() {
        return getString(ENCODING_CONFIG);
    }
}
