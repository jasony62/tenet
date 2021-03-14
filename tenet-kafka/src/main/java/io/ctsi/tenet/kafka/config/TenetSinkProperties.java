package io.ctsi.tenet.kafka.config;

import io.ctsi.tenet.kafka.connect.sink.TaskConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.PropertyMapper;

import java.util.HashMap;
import java.util.Map;

/**
 * 用于数据源配置
 */
@ConfigurationProperties(prefix = "tenet.sink")
public class TenetSinkProperties {

    private final Map<String, String> properties = new HashMap();

    public static final String METRICS_NUM_SAMPLES_CONFIG = CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG;
    public static final String METRICS_SAMPLE_WINDOW_MS_CONFIG = CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG;
    public static final String METRICS_RECORDING_LEVEL_CONFIG = CommonClientConfigs.METRICS_RECORDING_LEVEL_CONFIG;
    public static final String METRIC_REPORTER_CLASSES_CONFIG = CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG;
    private String dialectName;
    private String connectionUrl;
    private String connectionUser;
    private String connectionPassword;
    private String timezone;
    private String tableFormat;
    private String pkMode;
    private boolean autoCreate;
    private boolean autoEvolve;
    private int batchSize;
    private boolean deleteEnabled = false;
    private String insertMode;
    private int maxRetries;
    private int retryBackoff;
    private String topicName;
    private final String  TASK_CLASS = "io.ctsi.tenet.kafka.infrastructure.core.TenetWorkerTask";
    private String maxQueue;
    private String targetTopic;
    private String database;
    private String idStrategy;
    private int limitingEveryN;
    private int limitingTimeout;
    private String writeModelStrategy;
    private  long metricsSampleWindow;
    private int metricsNumSamples;
    private String metricsRecordingLevel;
    private String metricReporters;

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getDialectName() {
        return dialectName;
    }

    public void setDialectName(String dialectName) {
        this.dialectName = dialectName;
    }

    public String getConnectionUrl() {
        return connectionUrl;
    }

    public void setConnectionUrl(String connectionUrl) {
        this.connectionUrl = connectionUrl;
    }

    public String getConnectionUser() {
        return connectionUser;
    }

    public void setConnectionUser(String connectionUser) {
        this.connectionUser = connectionUser;
    }

    public String getConnectionPassword() {
        return connectionPassword;
    }

    public void setConnectionPassword(String connectionPassword) {
        this.connectionPassword = connectionPassword;
    }

    public String getTimezone() {
        return timezone;
    }

    public void setTimezone(String timezone) {
        this.timezone = timezone;
    }

    public String getTableFormat() {
        return tableFormat;
    }

    public void setTableFormat(String tableFormat) {
        this.tableFormat = tableFormat;
    }

    public String getPkMode() {
        return pkMode;
    }

    public void setPkMode(String pkMode) {
        this.pkMode = pkMode;
    }

    public boolean isAutoCreate() {
        return autoCreate;
    }

    public void setAutoCreate(boolean autoCreate) {
        this.autoCreate = autoCreate;
    }

    public boolean isAutoEvolve() {
        return autoEvolve;
    }

    public void setAutoEvolve(boolean autoEvolve) {
        this.autoEvolve = autoEvolve;
    }

    public String getBatchSize() {
        return String.valueOf(batchSize);
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public boolean isDeleteEnabled() {
        return deleteEnabled;
    }

    public void setDeleteEnabled(boolean deleteEnabled) {
        this.deleteEnabled = deleteEnabled;
    }

    public String getInsertMode() {
        return insertMode;
    }

    public void setInsertMode(String insertMode) {
        this.insertMode = insertMode;
    }

    public String getMaxRetries() {
        return String.valueOf(maxRetries);
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public String getRetryBackoff() {
        return String.valueOf(retryBackoff);
    }

    public void setRetryBackoff(int retryBackoff) {
        this.retryBackoff = retryBackoff;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public String getMaxQueue() {
        return maxQueue;
    }

    public void setMaxQueue(String maxQueue) {
        this.maxQueue = maxQueue;
    }

    public String getTargetTopic() {
        return targetTopic;
    }

    public void setTargetTopic(String targetTopic) {
        this.targetTopic = targetTopic;
    }

    public String getIdStrategy() {
        return idStrategy;
    }

    public void setIdStrategy(String idStrategy) {
        this.idStrategy = idStrategy;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public int getLimitingEveryN() {
        return limitingEveryN;
    }

    public void setLimitingEveryN(int limitingEveryN) {
        this.limitingEveryN = limitingEveryN;
    }

    public int getLimitingTimeout() {
        return limitingTimeout;
    }

    public void setLimitingTimeout(int limitingTimeout) {
        this.limitingTimeout = limitingTimeout;
    }

    public String getWriteModelStrategy() {
        return writeModelStrategy;
    }

    public void setWriteModelStrategy(String writeModelStrategy) {
        this.writeModelStrategy = writeModelStrategy;
    }

    private static class Properties extends HashMap<String, Object> {
        private Properties() {
        }

        <V> java.util.function.Consumer<V> in(String key) {
            return (value) -> {
                this.put(key, value);
            };
        }

        Properties with(Map<String, String> properties) {
            this.putAll(properties);
            return this;
        }
    }

    public long getMetricsSampleWindow() {
        return metricsSampleWindow;
    }

    public void setMetricsSampleWindow(long metricsSampleWindow) {
        this.metricsSampleWindow = metricsSampleWindow;
    }

    public int getMetricsNumSamples() {
        return metricsNumSamples;
    }

    public void setMetricsNumSamples(int metricsNumSamples) {
        this.metricsNumSamples = metricsNumSamples;
    }

    public String getMetricsRecordingLevel() {
        return metricsRecordingLevel;
    }

    public void setMetricsRecordingLevel(String metricsRecordingLevel) {
        this.metricsRecordingLevel = metricsRecordingLevel;
    }

    public String getMetricReporters() {
        return metricReporters;
    }

    public void setMetricReporters(String metricReporters) {
        this.metricReporters = metricReporters;
    }

    public Map<String, Object> buildProperties() {

        Properties properties = new Properties();
        PropertyMapper map = PropertyMapper.get().alwaysApplyingWhenNonNull();
        map.from(this::getConnectionUrl).to(properties.in("connection.url"));
        map.from(this::getConnectionUser).to(properties.in("connection.user"));
        map.from(this::getConnectionPassword).to(properties.in("connection.password"));
        map.from(this::isAutoCreate).as(String::valueOf).to(properties.in("auto.create"));
        map.from(this::isAutoEvolve).as(String::valueOf).to(properties.in("auto.evolve"));
        map.from(this::getBatchSize).to(properties.in("batch.size"));
        map.from(this::getPkMode).to(properties.in("pk.mode"));
        map.from(this::getTableFormat).to(properties.in("table.name.format"));
        map.from(this::getMaxRetries).to(properties.in("max.retries"));
        //MONGO
       // map.from(this::getMaxRetries).to(properties.in("max.num.retries"));
        map.from(this::getRetryBackoff).as(String::valueOf).to(properties.in("retry.backoff.ms"));
        map.from(this::getInsertMode).to(properties.in("insert.mode"));
        map.from(this::getTimezone).to(properties.in("db.timezone"));
        map.from(this::getTopicName).to(properties.in("topics"));
        map.from(this::getMaxQueue).to(properties.in("tenet.max.queue"));
        map.from(this::getTargetTopic).to(properties.in("tenet.target.topic"));
        map.from(this::getDatabase).to(properties.in("database"));
        map.from(this::getDialectName).to(properties.in("dialectName"));
        map.from(this::getIdStrategy).to(properties.in("document.id.strategy"));
        map.from(this::getWriteModelStrategy).to(properties.in("writemodel.strategy"));
        map.from(this::getLimitingEveryN).as(String::valueOf).to(properties.in("rate.limiting.every.n"));
        map.from(this::getLimitingTimeout).as(String::valueOf).to(properties.in("rate.limiting.timeout"));
        //map.from(this::getOffsetFlush).to(properties.in(TaskConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG));
        map.from(TASK_CLASS).to(properties.in(TaskConfig.TASK_CLASS_CONFIG));

        map.from(this::getMetricsSampleWindow).as(String::valueOf).to(properties.in(METRICS_SAMPLE_WINDOW_MS_CONFIG));
        map.from(this::getMetricsRecordingLevel).to(properties.in(METRICS_RECORDING_LEVEL_CONFIG));
        map.from(this::getMetricReporters).to(properties.in(METRIC_REPORTER_CLASSES_CONFIG));
        map.from(this::getMetricsNumSamples).as(String::valueOf).to(properties.in(METRICS_NUM_SAMPLES_CONFIG));
        return properties.with(this.properties);

    }

}
