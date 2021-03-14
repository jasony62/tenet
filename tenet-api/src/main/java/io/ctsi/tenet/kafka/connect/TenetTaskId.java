package io.ctsi.tenet.kafka.connect;

import java.io.Serializable;
import java.util.Objects;

public class TenetTaskId implements Serializable, Comparable<TenetTaskId> {
    private final String connector;
    private final int task;

    public TenetTaskId(String connector, int task) {
        this.connector = connector;
        this.task = task;
    }

    public String connector() {
        return this.connector;
    }

    public int task() {
        return this.task;
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            TenetTaskId that = (TenetTaskId)o;
            return this.task != that.task ? false : Objects.equals(this.connector, that.connector);
        } else {
            return false;
        }
    }

    public int hashCode() {
        int result = this.connector != null ? this.connector.hashCode() : 0;
        result = 31 * result + this.task;
        return result;
    }

    public String toString() {
        return this.connector + '-' + this.task;
    }

    public int compareTo(TenetTaskId o) {
        int connectorCmp = this.connector.compareTo(o.connector);
        return connectorCmp != 0 ? connectorCmp : Integer.compare(this.task, o.task);
    }
}
