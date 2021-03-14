package io.ctsi.tenet.framework.worker;


import com.google.gson.Gson;
import io.ctsi.tenet.framework.TenetMessage;
import io.ctsi.tenet.framework.TenetTask;
import io.ctsi.tenet.framework.config.TenetNodeTaskConfig;
import io.ctsi.tenet.kafka.task.BufferManager;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Mc.D
 */
public class TenetCoreThread implements Runnable{
    private static final Logger log = LoggerFactory.getLogger(BufferManager.class);

    private final   io.ctsi.tenet.framework.TenetNode node;
    private final TenetNodeTaskConfig config;
    private final TenetMessage message;

    @Getter
    @Setter
    private AtomicLong emptyCount = new AtomicLong(0);

    @Getter
    @Setter
    private AtomicLong runCount = new AtomicLong(0);

    public TenetCoreThread(TenetNodeTaskConfig config, io.ctsi.tenet.framework.TenetNode node, io.ctsi.tenet.framework.TenetMessage tenetMessage) {
        this.config = config;
        this.node = node;
        this.message = tenetMessage;
    }

    private final Gson gson = new Gson();


    @Override
    public void run() {

        /*long n = System.nanoTime();
        log.debug("runner start");
        //final TenetMessage message = node.pullData();
        if (message == null || ObjectUtils.isEmpty(message.getJsonData())) {
            log.debug("runner get nothing message... exit");
            log.debug("there are {} empty tasks in this time", emptyCount.incrementAndGet());
            return;
        }

        log.debug("there are {} normal tasks in this time", runCount.incrementAndGet());
        log.debug("runner get message:{}", message.getJsonData());
          io.ctsi.tenet.framework.TenetTask task = gson.fromJson(message.getJsonData(),   io.ctsi.tenet.framework.TenetTask.class);
        if (task == null) {
            task = new   io.ctsi.tenet.framework.TenetTask();
        }
        if (task.getJsonData() == null) {
            task.setJsonData(message.getJsonData());
        }
        saveTaskInfo(task);
        if (  io.ctsi.tenet.framework.TaskStatus.FAILIURE.name().equals(task.getTaskStatus())) {
            log.debug("drop wrong task");
            return;
        }
        if (config.getMaxRetryTime() <= task.getRetryTime()) {
           // task.setTaskStatus(  io.ctsi.tenet.framework.TaskStatus.FAILIURE.name());
            saveTaskInfo(task);
            log.debug("drop task for retry {} times", task.getRetryTime());
            return;
        }
        task.setTaskStatus(  io.ctsi.tenet.framework.TaskStatus.RUNNING.name());
        task.setTaskHolder(config.getHolder());
        saveTaskInfo(task);
        log.debug("task starting");
        if (node.getBefore() != null && !node.getBefore().apply(task)) {
            log.debug("task error in before action {}", task);
            task.setRetryTime(task.getRetryTime() + 1);
            task.setTaskStatus(  io.ctsi.tenet.framework.TaskStatus.RETRY.name());
            saveTaskInfo(task);
            message.setJsonData(gson.toJson(task));
            node.pushBack(message);
            return;
        }
        if (node.getRun() != null && !node.getRun().apply(task)) {
            log.debug("task error in run action {}", task);
            task.setRetryTime(task.getRetryTime() + 1);
            task.setTaskStatus(  io.ctsi.tenet.framework.TaskStatus.RETRY.name());
            message.setJsonData(gson.toJson(task));
            saveTaskInfo(task);
            node.pushBack(message);
            return;
        }
        if (node.getAfter() != null && !node.getAfter().apply(task)) {
            log.debug("task error in after action {}", task);
            task.setRetryTime(task.getRetryTime() + 1);
            task.setTaskStatus(  io.ctsi.tenet.framework.TaskStatus.RETRY.name());
            message.setJsonData(gson.toJson(task));
            saveTaskInfo(task);
            node.pushBack(message);
            return;
        }
        task.setTaskStatus(  io.ctsi.tenet.framework.TaskStatus.SUCCESS.name());
        saveTaskInfo(task);
        log.debug("task success {}", task);
        task.setTaskStatus(  io.ctsi.tenet.framework.TaskStatus.CREATE.name());
        task.setRetryTime(0);
        message.setJsonData(gson.toJson(task));
        node.pushNext(message);
        log.debug("task pushed next {}", task);
        long n2 = System.nanoTime();
        log.debug("task cost {}ns", n2 - n);*/
        TenetTask task = new TenetTask();

        if (node.getRun() != null ) {
            log.debug("task error in run action {}", task);
            task.setJsonData(message.getJsonData());
            saveTaskInfo(task);
            node.pushNext(task);
            return;
        }
    };
    private void saveTaskInfo(  io.ctsi.tenet.framework.TenetTask tenetTask) {
        log.debug("TODO saving task to mongoDB: {}", gson.toJson(tenetTask));
        // TODO save task to database
    }
}
