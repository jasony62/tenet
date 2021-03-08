package io.ctsi.tenet.kafka.connect.policy;

import io.ctsi.tenet.kafka.connect.error.ConnectException;
import io.ctsi.tenet.kafka.connect.error.RetriableException;
import io.ctsi.tenet.kafka.connect.metrics.ErrorHandlingMetrics;
import io.ctsi.tenet.kafka.connect.source.SourceRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class RetryWithToleranceOperator implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RetryWithToleranceOperator.class);

    public static final long RETRIES_DELAY_MIN_MS = 300;

    private static final Map<Stage, Class<? extends Exception>> TOLERABLE_EXCEPTIONS = new HashMap<>();
    static {
        TOLERABLE_EXCEPTIONS.put(Stage.TRANSFORMATION, Exception.class);
        TOLERABLE_EXCEPTIONS.put(Stage.HEADER_CONVERTER, Exception.class);
        TOLERABLE_EXCEPTIONS.put(Stage.KEY_CONVERTER, Exception.class);
        TOLERABLE_EXCEPTIONS.put(Stage.VALUE_CONVERTER, Exception.class);
    }

    private final long errorRetryTimeout;
    private final long errorMaxDelayInMillis;
    private final io.ctsi.tenet.kafka.connect.policy.ToleranceType errorToleranceType;

    private long totalFailures = 0;
    private final Time time;
    private ErrorHandlingMetrics errorHandlingMetrics;

    protected io.ctsi.tenet.kafka.connect.policy.ProcessingContext context = new io.ctsi.tenet.kafka.connect.policy.ProcessingContext();

    public RetryWithToleranceOperator(long errorRetryTimeout, long errorMaxDelayInMillis,
                                      io.ctsi.tenet.kafka.connect.policy.ToleranceType toleranceType, Time time) {
        this.errorRetryTimeout = errorRetryTimeout;
        this.errorMaxDelayInMillis = errorMaxDelayInMillis;
        this.errorToleranceType = toleranceType;
        this.time = time;
    }

    /**
     * Execute the recoverable operation. If the operation is already in a failed state, then simply return
     * with the existing failure.
     *
     * @param operation the recoverable operation
     * @param <V> return type of the result of the operation.
     * @return result of the operation
     */
    public <V> V execute(io.ctsi.tenet.kafka.connect.policy.Operation<V> operation, Stage stage, Class<?> executingClass) {
        context.currentContext(stage, executingClass);

        if (context.failed()) {
            log.debug("ProcessingContext is already in failed state. Ignoring requested operation.");
            return null;
        }

        try {
            Class<? extends Exception> ex = TOLERABLE_EXCEPTIONS.getOrDefault(context.stage(), RetriableException.class);
            return execAndHandleError(operation, ex);
        } finally {
            if (context.failed()) {
                //errorHandlingMetrics.recordError();
                context.report();
            }
        }
    }

    /**
     * Attempt to execute an operation. Retry if a {@link RetriableException} is raised. Re-throw everything else.
     *
     * @param operation the operation to be executed.
     * @param <V> the return type of the result of the operation.
     * @return the result of the operation.
     * @throws Exception rethrow if a non-retriable Exception is thrown by the operation
     */
    protected <V> V execAndRetry(io.ctsi.tenet.kafka.connect.policy.Operation<V> operation) throws Exception {
        int attempt = 0;
        long startTime = time.milliseconds();
        long deadline = startTime + errorRetryTimeout;
        do {
            try {
                attempt++;
                return operation.call();
            } catch (RetriableException e) {
                log.trace("Caught a retriable exception while executing {} operation with {}", context.stage(), context.executingClass());
                //errorHandlingMetrics.recordFailure();
                if (checkRetry(startTime)) {
                    backoff(attempt, deadline);
                    if (Thread.currentThread().isInterrupted()) {
                        log.trace("Thread was interrupted. Marking operation as failed.");
                        context.error(e);
                        return null;
                    }
                    //errorHandlingMetrics.recordRetry();
                } else {
                    log.trace("Can't retry. start={}, attempt={}, deadline={}", startTime, attempt, deadline);
                    context.error(e);
                    return null;
                }
            } finally {
                context.attempt(attempt);
            }
        } while (true);
    }

    /**
     * Execute a given operation multiple times (if needed), and tolerate certain exceptions.
     *
     * @param operation the operation to be executed.
     * @param tolerated the class of exceptions which can be tolerated.
     * @param <V> The return type of the result of the operation.
     * @return the result of the operation
     */
    // Visible for testing
    protected <V> V execAndHandleError(io.ctsi.tenet.kafka.connect.policy.Operation<V> operation, Class<? extends Exception> tolerated) {
        try {
            V result = execAndRetry(operation);
            if (context.failed()) {
                markAsFailed();
                errorHandlingMetrics.recordSkipped();
            }
            return result;
        } catch (Exception e) {
            errorHandlingMetrics.recordFailure();
            markAsFailed();
            context.error(e);

            if (!tolerated.isAssignableFrom(e.getClass())) {
                throw new ConnectException("Unhandled exception in error handler", e);
            }

            if (!withinToleranceLimits()) {
                throw new ConnectException("Tolerance exceeded in error handler", e);
            }

            errorHandlingMetrics.recordSkipped();
            return null;
        }
    }

    // Visible for testing
    void markAsFailed() {
        //errorHandlingMetrics.recordErrorTimestamp();
        totalFailures++;
    }

    // Visible for testing
    @SuppressWarnings("fallthrough")
    public boolean withinToleranceLimits() {
        switch (errorToleranceType) {
            case NONE:
                if (totalFailures > 0) return false;
            case ALL:
                return true;
            default:
                throw new ConfigException("Unknown tolerance type: {}", errorToleranceType);
        }
    }

    // Visible for testing
    boolean checkRetry(long startTime) {
        return (time.milliseconds() - startTime) < errorRetryTimeout;
    }

    // Visible for testing
    void backoff(int attempt, long deadline) {
        int numRetry = attempt - 1;
        long delay = RETRIES_DELAY_MIN_MS << numRetry;
        if (delay > errorMaxDelayInMillis) {
            delay = ThreadLocalRandom.current().nextLong(errorMaxDelayInMillis);
        }
        if (delay + time.milliseconds() > deadline) {
            delay = deadline - time.milliseconds();
        }
        log.debug("Sleeping for {} millis", delay);
        time.sleep(delay);
    }

    public void metrics(ErrorHandlingMetrics errorHandlingMetrics) {
        this.errorHandlingMetrics = errorHandlingMetrics;
    }

    @Override
    public String toString() {
        return "RetryWithToleranceOperator{" +
                "errorRetryTimeout=" + errorRetryTimeout +
                ", errorMaxDelayInMillis=" + errorMaxDelayInMillis +
                ", errorToleranceType=" + errorToleranceType +
                ", totalFailures=" + totalFailures +
                ", time=" + time +
                ", context=" + context +
                '}';
    }

    /**
     * Set the error reporters for this connector.
     *
     * @param reporters the error reporters (should not be null).
     */
    public void reporters(List<io.ctsi.tenet.kafka.connect.policy.ErrorReporter> reporters) {
        this.context.reporters(reporters);
    }

    /**
     * Set the source record being processed in the connect pipeline.
     *
     * @param preTransformRecord the source record
     */
    public void sourceRecord(SourceRecord preTransformRecord) {
        this.context.sourceRecord(preTransformRecord);
    }

    /**
     * Set the record consumed from Kafka in a sink connector.
     *
     * @param consumedMessage the record
     */
    public void consumerRecord(ConsumerRecord<byte[], byte[]> consumedMessage) {
        this.context.consumerRecord(consumedMessage);
    }

    /**
     * @return true, if the last operation encountered an error; false otherwise
     */
    public boolean failed() {
        return this.context.failed();
    }

    @Override
    public void close() {
        this.context.close();
    }

    public synchronized Throwable error() {
        return this.context.error();
    }

}
