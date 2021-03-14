package io.ctsi.tenet.framework;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Mc.D
 */
public class MapTenetTaskCache implements   io.ctsi.tenet.framework.TenetTaskCache {

    private final ConcurrentHashMap<String,   io.ctsi.tenet.framework.TenetTask> map = new ConcurrentHashMap<>();

    @Override
    public   io.ctsi.tenet.framework.TenetTask getTask(String taskId) {
          io.ctsi.tenet.framework.TenetTask task = map.get(taskId);
        if (task != null) {
            map.remove(taskId);
        }
        return task;
    }

    @Override
    public void setCache(  io.ctsi.tenet.framework.TenetTask tenetTask) {
        map.put(tenetTask.getTaskId(), tenetTask);
    }
}
