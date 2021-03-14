package io.ctsi.tenet.kafka.mongodb.util;

import org.apache.kafka.common.config.ConfigException;

public class ConnectConfigException extends ConfigException {
    private final String name;
    private final Object value;

    private final String originalMessage;

    public ConnectConfigException(final String name, final Object value, final String message) {
        super(name, value, message);
        this.originalMessage = message;
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public Object getValue() {
        return value;
    }

    public String getOriginalMessage() {
        return originalMessage;
    }
}

