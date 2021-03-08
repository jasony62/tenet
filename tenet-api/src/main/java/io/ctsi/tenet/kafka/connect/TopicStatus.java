package io.ctsi.tenet.kafka.connect;

import java.util.Objects;

public class TopicStatus {
    private final String topic;
    private final String connector;
    private final int task;
    private final long discoverTimestamp;

    public TopicStatus(String topic, TenetTaskId task, long discoverTimestamp) {
        this(topic, task.connector(), task.task(), discoverTimestamp);
    }

    public TopicStatus(String topic, String connector, int task, long discoverTimestamp) {
        this.topic = Objects.requireNonNull(topic);
        this.connector = Objects.requireNonNull(connector);
        this.task = task;
        this.discoverTimestamp = discoverTimestamp;
    }

    /**
     * Get the name of the topic.
     *
     * @return the topic name; never null
     */
    public String topic() {
        return topic;
    }

    /**
     * Get the name of the connector.
     *
     * @return the connector name; never null
     */
    public String connector() {
        return connector;
    }

    /**
     * Get the ID of the task that stored the topic status.
     *
     * @return the task ID
     */
    public int task() {
        return task;
    }

    /**
     * Get a timestamp that represents when this topic was discovered as being actively used by
     * this connector.
     *
     * @return the discovery timestamp
     */
    public long discoverTimestamp() {
        return discoverTimestamp;
    }

    @Override
    public String toString() {
        return "TopicStatus{" +
                "topic='" + topic + '\'' +
                ", connector='" + connector + '\'' +
                ", task=" + task +
                ", discoverTimestamp=" + discoverTimestamp +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TopicStatus)) {
            return false;
        }
        TopicStatus that = (TopicStatus) o;
        return task == that.task &&
                discoverTimestamp == that.discoverTimestamp &&
                topic.equals(that.topic) &&
                connector.equals(that.connector);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, connector, task, discoverTimestamp);
    }
}
