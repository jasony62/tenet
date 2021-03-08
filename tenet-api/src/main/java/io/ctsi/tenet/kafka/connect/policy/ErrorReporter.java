package io.ctsi.tenet.kafka.connect.policy;

public interface ErrorReporter extends AutoCloseable {

    /**
     * Report an error.
     *
     * @param context the processing context (cannot be null).
     */
    void report(ProcessingContext context);

    @Override
    default void close() { }
}
