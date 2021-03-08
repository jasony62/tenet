package io.ctsi.tenet.kafka.connect.policy;

import java.util.Locale;

public enum ToleranceType {
    /**
     * Tolerate no errors.
     */
    NONE,

    /**
     * Tolerate all errors.
     */
    ALL;

    public String value() {
        return name().toLowerCase(Locale.ROOT);
    }

}
