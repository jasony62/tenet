package io.ctsi.tenet.framework;

import com.google.gson.Gson;
import io.ctsi.tenet.framework.config.TenetNodeTaskConfig;
import io.ctsi.tenet.kafka.connect.sink.InternalSinkRecord;
import io.ctsi.tenet.kafka.task.BufferManager;
import io.ctsi.tenet.kafka.task.QueueManager;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * @author Mc.D
 */
@SuppressWarnings({"SpringJavaAutowiredMembersInspection"})
@Slf4j
public abstract class AbstractKafkaTenetNode implements TenetNode {

    @Getter
    private final   io.ctsi.tenet.framework.TenetTaskDelayQueue dq = new   io.ctsi.tenet.framework.TenetTaskDelayQueue();

    @Lazy
    @Autowired
    protected TenetNodeTaskConfig config;

    @Lazy
    @Autowired
    protected QueueManager qm;

    @Lazy
    @Autowired
    protected BufferManager bm;

    private final Gson gson = new Gson();

    @Override
    @Deprecated
    public final TenetTask pullData() {

        /*try {
            TenetTask tm = dq.poll();
            if (tm != null) {
                return tm;
            }
            InternalSinkRecord record = (InternalSinkRecord) qm.pullData(0, null);
            if (record != null) {
                String data = new String(record.originalRecord().value(), StandardCharsets.UTF_8);
                TenetTask task = gson.fromJson(data, TenetTask.class);
                if (task == null) {
                    task = new TenetTask();
                }
                if (task.getData() == null) {
                    task.setData(data);
                }
                return task;
            }
        } catch (Exception e) {
            log.trace("", e);
        }
        log.debug("no data pulled");*/
        return null;
    }

    @Override
    public final void pushBack(TenetTask task) {        dq.offer(task, config.getRetryDelay(), TimeUnit.MILLISECONDS);

    }

    @Override
    public final void pushNext(TenetTask task) {
        bm.addMessage(gson.toJson(task));
    }
}
