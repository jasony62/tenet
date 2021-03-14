package io.ctsi.tenet.kafka.infrastructure.core;

import io.ctsi.tenet.kafka.connect.TenetTaskId;
import io.ctsi.tenet.kafka.connect.storage.MemoryStatusBackingStore;
import io.ctsi.tenet.kafka.connect.sink.TaskStatus;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

/**
 * 将线程的每个状态更新到statusBackingStore类似的map结构
 * 用于WorkerTask线程statusListener
 * 可用于监控线程状态
 */
public class TaskHerder implements TaskStatus.Listener {

    private final MemoryStatusBackingStore statusBackingStore;
    private final String workerId;

    public TaskHerder(MemoryStatusBackingStore memoryStatusBackingStore, String workerId) {
        this.statusBackingStore = memoryStatusBackingStore;
        this.workerId = workerId;
    }

    @Override
    public void onStartup(TenetTaskId id) {
        statusBackingStore.put(new TaskStatus(id, TaskStatus.State.RUNNING, workerId, 0));
    }

    @Override
    public void onFailure(TenetTaskId id, Throwable cause) {
        statusBackingStore.putSafe(new TaskStatus(id, TaskStatus.State.FAILED, workerId, 0, trace(cause)));
    }

    @Override
    public void onShutdown(TenetTaskId id) {
        statusBackingStore.putSafe(new TaskStatus(id, TaskStatus.State.UNASSIGNED, workerId, 0));
    }

    @Override
    public void onResume(TenetTaskId id) {
        statusBackingStore.put(new TaskStatus(id, TaskStatus.State.RUNNING, workerId, 0));
    }

    @Override
    public void onPause(TenetTaskId id) {
        statusBackingStore.put(new TaskStatus(id, TaskStatus.State.PAUSED, workerId, 0));
    }

    @Override
    public void onDeletion(TenetTaskId id) {
        statusBackingStore.put(new TaskStatus(id, TaskStatus.State.DESTROYED, workerId, 0));
    }

    private String trace(Throwable t) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try {
            t.printStackTrace(new PrintStream(output, false, StandardCharsets.UTF_8.name()));
            return output.toString("UTF-8");
        } catch (UnsupportedEncodingException e) {
            return null;
        }
    }

}
