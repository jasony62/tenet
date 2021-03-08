package io.ctsi.tenet.framework;

/**
 * @author Mc.D
 */
public interface TenetTaskCache {
    TenetTask getTask(String taskId);
    void setCache(TenetTask tenetTask);
}
