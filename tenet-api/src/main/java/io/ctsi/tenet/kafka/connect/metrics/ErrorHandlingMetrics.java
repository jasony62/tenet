package io.ctsi.tenet.kafka.connect.metrics;

import io.ctsi.tenet.kafka.connect.TenetTaskId;
import io.ctsi.tenet.kafka.connect.policy.CumulativeSum;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;

import java.util.ArrayList;

public class ErrorHandlingMetrics {
    private final Time time = new SystemTime();

    private final ConnectMetrics.MetricGroup metricGroup;

    // metrics
    private final Sensor recordProcessingFailures;
    private final Sensor recordProcessingErrors;
    private final Sensor recordsSkipped;
    private final Sensor retries;
    private final Sensor errorsLogged;
    private final Sensor dlqProduceRequests;
    private final Sensor dlqProduceFailures;
    private long lastErrorTime = 0;

    // for testing only
    public ErrorHandlingMetrics() {
        this(new TenetTaskId("noop-connector", -1),
                new ConnectMetrics("noop-worker", new SystemTime(), 2, 3000, Sensor.RecordingLevel.INFO.toString(),
                        new ArrayList<>()));
    }

    public ErrorHandlingMetrics(TenetTaskId id, ConnectMetrics connectMetrics) {

        io.ctsi.tenet.kafka.connect.metrics.ConnectMetricsRegistry registry = connectMetrics.registry();
        metricGroup = connectMetrics.group(registry.taskErrorHandlingGroupName(),
                registry.connectorTagName(), id.connector(), registry.taskTagName(), Integer.toString(id.task()));

        // prevent collisions by removing any previously created metrics in this group.
        metricGroup.close();

        recordProcessingFailures = metricGroup.sensor("total-record-failures");
        recordProcessingFailures.add(metricGroup.metricName(registry.recordProcessingFailures), new CumulativeSum());

        recordProcessingErrors = metricGroup.sensor("total-record-errors");
        recordProcessingErrors.add(metricGroup.metricName(registry.recordProcessingErrors), new CumulativeSum());

        recordsSkipped = metricGroup.sensor("total-records-skipped");
        recordsSkipped.add(metricGroup.metricName(registry.recordsSkipped), new CumulativeSum());

        retries = metricGroup.sensor("total-retries");
        retries.add(metricGroup.metricName(registry.retries), new CumulativeSum());

        errorsLogged = metricGroup.sensor("total-errors-logged");
        errorsLogged.add(metricGroup.metricName(registry.errorsLogged), new CumulativeSum());

        dlqProduceRequests = metricGroup.sensor("deadletterqueue-produce-requests");
        dlqProduceRequests.add(metricGroup.metricName(registry.dlqProduceRequests), new CumulativeSum());

        dlqProduceFailures = metricGroup.sensor("deadletterqueue-produce-failures");
        dlqProduceFailures.add(metricGroup.metricName(registry.dlqProduceFailures), new CumulativeSum());

        metricGroup.addValueMetric(registry.lastErrorTimestamp, now -> lastErrorTime);
    }

    /**
     * Increment the number of failed operations (retriable and non-retriable).
     */
    public void recordFailure() {
        recordProcessingFailures.record();
    }

    /**
     * Increment the number of operations which could not be successfully executed.
     */
    public void recordError() {
        recordProcessingErrors.record();
    }

    /**
     * Increment the number of records skipped.
     */
    public void recordSkipped() {
        recordsSkipped.record();
    }

    /**
     * The number of retries made while executing operations.
     */
    public void recordRetry() {
        retries.record();
    }

    /**
     * The number of errors logged by the {@link LogReporter}.
     */
    public void recordErrorLogged() {
        errorsLogged.record();
    }

    /**
     * The number of produce requests to the {@link DeadLetterQueueReporter}.
     */
    public void recordDeadLetterQueueProduceRequest() {
        dlqProduceRequests.record();
    }

    /**
     * The number of produce requests to the {@link DeadLetterQueueReporter} which failed to be successfully produced into Kafka.
     */
    public void recordDeadLetterQueueProduceFailed() {
        dlqProduceFailures.record();
    }

    /**
     * Record the time of error.
     */
    public void recordErrorTimestamp() {
        this.lastErrorTime = time.milliseconds();
    }

    /**
     * @return the metric group for this class.
     */
    public ConnectMetrics.MetricGroup metricGroup() {
        return metricGroup;
    }
}
