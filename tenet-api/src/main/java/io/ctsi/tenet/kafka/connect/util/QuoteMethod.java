package io.ctsi.tenet.kafka.connect.util;

public enum QuoteMethod {
    ALWAYS("always"),
    NEVER("never");

    public static QuoteMethod get(String name) {
        for (QuoteMethod method : values()) {
            if (method.toString().equalsIgnoreCase(name)) {
                return method;
            }
        }
        throw new IllegalArgumentException("No matching QuoteMethod found for '" + name + "'");
    }

    private final String name;

    QuoteMethod(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }
}
