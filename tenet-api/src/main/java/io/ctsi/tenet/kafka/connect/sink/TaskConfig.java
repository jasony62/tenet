package io.ctsi.tenet.kafka.connect.sink;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.HashMap;
import java.util.Map;

public class TaskConfig extends AbstractConfig {

    public static final String TASK_CLASS_CONFIG = "task.class";
    private static final String TASK_CLASS_DOC =
            "Name of the class for this task. Must be a subclass of org.apache.kafka.connect.connector.Task";

    public static final String OFFSET_COMMIT_INTERVAL_MS_CONFIG ="offset.flush.interval.ms";

    private static ConfigDef config;

    static {
        config = new ConfigDef()
                .define(TASK_CLASS_CONFIG, ConfigDef.Type.CLASS, ConfigDef.Importance.HIGH, TASK_CLASS_DOC);
    }

    public TaskConfig() {
        this(new HashMap<String, String>());
    }

    public TaskConfig(Map<String, ?> props) {
        super(config, props, true);
    }
}

