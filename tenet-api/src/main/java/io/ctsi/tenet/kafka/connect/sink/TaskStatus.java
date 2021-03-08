package io.ctsi.tenet.kafka.connect.sink;

import io.ctsi.tenet.kafka.connect.AbstractStatus;
import io.ctsi.tenet.kafka.connect.TenetTaskId;

public class TaskStatus extends AbstractStatus<TenetTaskId> {
    public TaskStatus(TenetTaskId id, State state, String workerUrl, int generation, String trace) {
        super(id, state, workerUrl, generation, trace);
    }

    public TaskStatus(TenetTaskId id, State state, String workerUrl, int generation) {
        super(id, state, workerUrl, generation, (String) null);
    }

    public interface Listener {
        void onStartup(TenetTaskId var1);

        void onPause(TenetTaskId var1);

        void onResume(TenetTaskId var1);

        void onFailure(TenetTaskId var1, Throwable var2);

        void onShutdown(TenetTaskId var1);

        void onDeletion(TenetTaskId var1);
    }

}
