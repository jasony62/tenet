package io.ctsi.tenet.kafka.task;

import io.ctsi.tenet.kafka.connect.TenetTaskId;
import io.ctsi.tenet.kafka.connect.metrics.ConnectMetrics;
import io.ctsi.tenet.kafka.connect.metrics.ConnectMetricsRegistry;
import io.ctsi.tenet.kafka.connect.sink.TaskConfig;
import io.ctsi.tenet.kafka.connect.sink.WorkerTask;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Frequencies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.SmartLifecycle;
import org.springframework.util.Assert;
import org.springframework.util.ConcurrentReferenceHashMap;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TenetWorkerTaskContainer  {

    private static final Logger log = LoggerFactory.getLogger(TenetWorkerTaskContainer.class);

    protected Map<String, Object> buildProperties;

    private WorkerMetricsGroup workerMetricsGroup;

    protected ConnectMetrics connectMetrics;

    protected ConcurrentReferenceHashMap<TenetTaskId, WorkerTask> taskContainer = new ConcurrentReferenceHashMap(16);

    public void registerTenetMessageTask(WorkerTask tenetMessageTask){

        taskContainer.put(tenetMessageTask.id(),tenetMessageTask);
        try {
            tenetMessageTask.initialize(new TaskConfig(buildProperties));
        }catch (RuntimeException re){
            Assert.notNull(re, "tenetMessageTask is error");
            taskContainer.clear();
        }
    }

    public void setConnectMetricsAndWorkerMetricsGroup(ConnectMetrics connectMetrics) {
        this.connectMetrics = connectMetrics;
        workerMetricsGroup = new WorkerMetricsGroup(connectMetrics);
    }

    public ConcurrentReferenceHashMap<TenetTaskId, WorkerTask> getTaskContainer() {
        return taskContainer;
    }

    public WorkerMetricsGroup getWorkerMetricsGroup() {
        return workerMetricsGroup;
    }

    public void setBuildProperties(Map<String, Object> buildProperties) {
        this.buildProperties = buildProperties;
    }

    public class WorkerMetricsGroup {
        private final ConnectMetrics.MetricGroup metricGroup;
        private final Sensor connectorStartupAttempts;
        private final Sensor connectorStartupSuccesses;
        private final Sensor connectorStartupFailures;
        private final Sensor connectorStartupResults;
        private final Sensor taskStartupAttempts;
        private final Sensor taskStartupSuccesses;
        private final Sensor taskStartupFailures;
        private final Sensor taskStartupResults;

        public WorkerMetricsGroup(ConnectMetrics connectMetrics) {
            ConnectMetricsRegistry registry = connectMetrics.registry();
            metricGroup = connectMetrics.group(registry.workerGroupName());

            //metricGroup.addValueMetric(registry.connectorCount, now -> (double) connectors.size());
            metricGroup.addValueMetric(registry.taskCount, now -> (double) taskContainer.size());

            MetricName connectorFailurePct = metricGroup.metricName(registry.connectorStartupFailurePercentage);
            MetricName connectorSuccessPct = metricGroup.metricName(registry.connectorStartupSuccessPercentage);
            Frequencies connectorStartupResultFrequencies = Frequencies.forBooleanValues(connectorFailurePct, connectorSuccessPct);
            connectorStartupResults = metricGroup.sensor("connector-startup-results");
            connectorStartupResults.add(connectorStartupResultFrequencies);

            connectorStartupAttempts = metricGroup.sensor("connector-startup-attempts");
            connectorStartupAttempts.add(metricGroup.metricName(registry.connectorStartupAttemptsTotal), new CumulativeSum());

            connectorStartupSuccesses = metricGroup.sensor("connector-startup-successes");
            connectorStartupSuccesses.add(metricGroup.metricName(registry.connectorStartupSuccessTotal), new CumulativeSum());

            connectorStartupFailures = metricGroup.sensor("connector-startup-failures");
            connectorStartupFailures.add(metricGroup.metricName(registry.connectorStartupFailureTotal), new CumulativeSum());

            MetricName taskFailurePct = metricGroup.metricName(registry.taskStartupFailurePercentage);
            MetricName taskSuccessPct = metricGroup.metricName(registry.taskStartupSuccessPercentage);
            Frequencies taskStartupResultFrequencies = Frequencies.forBooleanValues(taskFailurePct, taskSuccessPct);
            taskStartupResults = metricGroup.sensor("task-startup-results");
            taskStartupResults.add(taskStartupResultFrequencies);

            taskStartupAttempts = metricGroup.sensor("task-startup-attempts");
            taskStartupAttempts.add(metricGroup.metricName(registry.taskStartupAttemptsTotal), new CumulativeSum());

            taskStartupSuccesses = metricGroup.sensor("task-startup-successes");
            taskStartupSuccesses.add(metricGroup.metricName(registry.taskStartupSuccessTotal), new CumulativeSum());

            taskStartupFailures = metricGroup.sensor("task-startup-failures");
            taskStartupFailures.add(metricGroup.metricName(registry.taskStartupFailureTotal), new CumulativeSum());
        }

        public void close() {
            metricGroup.close();
        }

        public void recordConnectorStartupFailure() {
            connectorStartupAttempts.record(1.0);
            connectorStartupFailures.record(1.0);
            connectorStartupResults.record(0.0);
        }

        public void recordConnectorStartupSuccess() {
            connectorStartupAttempts.record(1.0);
            connectorStartupSuccesses.record(1.0);
            connectorStartupResults.record(1.0);
        }

        public void recordTaskFailure() {
            taskStartupAttempts.record(1.0);
            taskStartupFailures.record(1.0);
            taskStartupResults.record(0.0);
        }

        public void recordTaskSuccess() {
            taskStartupAttempts.record(1.0);
            taskStartupSuccesses.record(1.0);
            taskStartupResults.record(1.0);
        }

        protected ConnectMetrics.MetricGroup metricGroup() {
            return metricGroup;
        }
    }

}

