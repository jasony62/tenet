package io.ctsi.tenet.framework;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * @author Mc.D
 */
@SuppressWarnings({"SpringJavaAutowiredMembersInspection"})
@Slf4j
public abstract class AbstractKafkaAsyncTenetNode extends   io.ctsi.tenet.framework.AbstractKafkaTenetNode implements   io.ctsi.tenet.framework.TenetNode {

    @Lazy
    @Autowired
    protected   io.ctsi.tenet.framework.TenetTaskCache ttc;

    public void wakeTask(String taskId) {
          io.ctsi.tenet.framework.TenetTask tenetTask = ttc.getTask(taskId);
        getDq().remove(tenetTask);
        getDq().offer(tenetTask, 0, NANOSECONDS);
    }
}

