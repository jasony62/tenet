package io.ctsi.tenet.kafka.connect.storage;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public enum ConverterType {
    KEY,
    VALUE,
    HEADER;

    private static final Map<String, ConverterType> NAME_TO_TYPE;

    static {
        ConverterType[] types = ConverterType.values();
        Map<String, ConverterType> nameToType = new HashMap<>(types.length);
        for (ConverterType type : types) {
            nameToType.put(type.name, type);
        }
        NAME_TO_TYPE = Collections.unmodifiableMap(nameToType);
    }

    /**
     * Find the ConverterType with the given name, using a case-insensitive match.
     * @param name the name of the converter type; may be null
     * @return the matching converter type, or null if the supplied name is null or does not match the name of the known types
     */
    public static ConverterType withName(String name) {
        if (name == null) {
            return null;
        }
        return NAME_TO_TYPE.get(name.toLowerCase(Locale.getDefault()));
    }

    private String name;

    ConverterType() {
        this.name = this.name().toLowerCase(Locale.ROOT);
    }

    public String getName() {
        return name;
    }
}
