package io.ctsi.tenet.kafka.connect.util;

import io.ctsi.tenet.kafka.connect.TenetTaskId;
import org.slf4j.MDC;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public class LoggingContext implements AutoCloseable {

    /**
     * The name of the Mapped Diagnostic Context (MDC) key that defines the context for a connector.
     */
    public static final String CONNECTOR_CONTEXT = "connector.context";

    public static final Collection<String> ALL_CONTEXTS = Collections.singleton(CONNECTOR_CONTEXT);

    /**
     * The Scope values used by Connect when specifying the context.
     */
    public enum Scope {
        /**
         * The scope value for the worker as it starts a connector.
         */
        WORKER("worker"),

        /**
         * The scope value for Task implementations.
         */
        TASK("task"),

        /**
         * The scope value for committing offsets.
         */
        OFFSETS("offsets"),

        /**
         * The scope value for validating connector configurations.
         */
        VALIDATE("validate");

        private final String text;
        Scope(String value) {
            this.text = value;
        }

        @Override
        public String toString() {
            return text;
        }
    }

    /**
     * Clear all MDC parameters.
     */
    public static void clear() {
        MDC.clear();
    }

    /**
     * Modify the current {@link MDC} logging context to set the {@link #CONNECTOR_CONTEXT connector context} to include the
     * supplied name and the {@link Scope#WORKER} scope.
     *
     * @param connectorName the connector name; may not be null
     */
    public static LoggingContext forConnector(String connectorName) {
        Objects.requireNonNull(connectorName);
        LoggingContext context = new LoggingContext();
        MDC.put(CONNECTOR_CONTEXT, prefixFor(connectorName, Scope.WORKER, null));
        return context;
    }

    /**
     * Modify the current {@link MDC} logging context to set the {@link #CONNECTOR_CONTEXT connector context} to include the
     * supplied connector name and the {@link Scope#VALIDATE} scope.
     *
     * @param connectorName the connector name
     */
    public static LoggingContext forValidation(String connectorName) {
        LoggingContext context = new LoggingContext();
        MDC.put(CONNECTOR_CONTEXT, prefixFor(connectorName, Scope.VALIDATE, null));
        return context;
    }

    /**
     * Modify the current {@link MDC} logging context to set the {@link #CONNECTOR_CONTEXT connector context} to include the
     * connector name and task number using the supplied {@link TenetTaskId}, and to set the scope to {@link Scope#TASK}.
     *
     * @param id the connector task ID; may not be null
     */
    public static LoggingContext forTask(TenetTaskId id) {
        Objects.requireNonNull(id);
        LoggingContext context = new LoggingContext();
        MDC.put(CONNECTOR_CONTEXT, prefixFor(id.connector(), Scope.TASK, id.task()));
        return context;
    }

    /**
     * Modify the current {@link MDC} logging context to set the {@link #CONNECTOR_CONTEXT connector context} to include the
     * connector name and task number using the supplied {@link TenetTaskId}, and to set the scope to {@link Scope#OFFSETS}.
     *
     * @param id the connector task ID; may not be null
     */
    public static LoggingContext forOffsets(TenetTaskId id) {
        Objects.requireNonNull(id);
        LoggingContext context = new LoggingContext();
        MDC.put(CONNECTOR_CONTEXT, prefixFor(id.connector(), Scope.OFFSETS, id.task()));
        return context;
    }

    /**
     * Return the prefix that uses the specified connector name, task number, and scope. The
     * format is as follows:
     *
     * <pre>
     *     [&lt;connectorName>|&lt;scope>]&lt;sp>
     * </pre>
     *
     * where "<code>&lt;connectorName></code>" is the name of the connector,
     * "<code>&lt;sp></code>" indicates a trailing space, and
     * "<code>&lt;scope></code>" is one of the following:
     *
     * <ul>
     *   <li>"<code>task-n</code>" for the operation of the numbered task, including calling the
     *      task methods and the producer/consumer; here "n" is the 0-based task number
     *   <li>"<code>task-n|offset</code>" for the committing of source offsets for the numbered
     *       task; here "n" is the * zero-based task number
     *   <li>"<code>worker</code>" for the creation and usage of connector instances
     * </ul>
     *
     * <p>The following are examples of the connector context for a connector named "my-connector":
     *
     * <ul>
     *   <li>`[my-connector|worker]` - used on log messages where the Connect worker is
     *     validating the configuration for or starting/stopping the "local-file-source" connector
     *     via the SourceConnector / SinkConnector implementation methods.
     *   <li>`[my-connector|task-0]` - used on log messages where the Connect worker is executing
     *     task 0 of the "local-file-source" connector, including calling any of the SourceTask /
     *     SinkTask implementation methods, processing the messages for/from the task, and
     *     calling the task's * producer/consumer.
     *   <li>`[my-connector|task-0|offsets]` - used on log messages where the Connect worker is
     *       committing * source offsets for task 0 of the "local-file-source" connector.
     * </ul>
     *
     * @param connectorName the name of the connector; may not be null
     * @param scope the scope; may not be null
     * @param taskNumber the 0-based task number; may be null if there is no associated task
     * @return the prefix; never null
     */
    protected static String prefixFor(String connectorName, Scope scope, Integer taskNumber) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        sb.append(connectorName);
        if (taskNumber != null) {
            // There is a task number, so this is a task
            sb.append("|");
            sb.append(Scope.TASK.toString());
            sb.append("-");
            sb.append(taskNumber.toString());
        }
        // Append non-task scopes (e.g., worker and offset)
        if (scope != Scope.TASK) {
            sb.append("|");
            sb.append(scope.toString());
        }
        sb.append("] ");
        return sb.toString();
    }

    private final Map<String, String> previous;

    private LoggingContext() {
        previous = MDC.getCopyOfContextMap(); // may be null!
    }

    /**
     * Close this logging context, restoring the Connect {@link MDC} parameters back to the state
     * just before this context was created. This does not affect other MDC parameters set by
     * connectors or tasks.
     */
    @Override
    public void close() {
        for (String param : ALL_CONTEXTS) {
            if (previous != null && previous.containsKey(param)) {
                MDC.put(param, previous.get(param));
            } else {
                MDC.remove(param);
            }
        }
    }
}