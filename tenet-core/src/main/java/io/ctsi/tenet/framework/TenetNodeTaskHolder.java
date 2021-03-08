package io.ctsi.tenet.framework;

import   io.ctsi.tenet.framework.config.TenetNodeTaskConfig;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.ObjectUtils;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;


/**
 * @author Mc.D
 */
@Slf4j
public class TenetNodeTaskHolder {

    private final TenetNode node;
    private final TenetNodeTaskConfig config;
    private final   io.ctsi.tenet.framework.TenetTaskCache tenetTaskCache;


    @Getter
    private final AtomicLong emptyCount = new AtomicLong(0);

    @Getter
    private final AtomicLong runCount = new AtomicLong(0);

    public TenetNodeTaskHolder(TenetNodeTaskConfig config, TenetNode node, @Autowired(required = false)   io.ctsi.tenet.framework.TenetTaskCache cache) {
        this.config = config;
        this.node = node;
        this.tenetTaskCache = cache;
    }
    @Deprecated
    /**
     * copy to TenetCoreThread
     */
    public Runnable getTask() {
        return () -> {
            final TenetTask task;
            long n = System.nanoTime();
            log.debug("runner start");
            try {
                task = node.pullData();
                //if (task == null || ObjectUtils.isEmpty(task.getData())) {
                if (task == null ) {
                    log.debug("runner get nothing message... exit");
                    log.debug("there are {} empty tasks in this time", emptyCount.incrementAndGet());
                    return;
                }
                log.debug("there are {} normal tasks in this time", runCount.incrementAndGet());
                log.debug("runner get message:{}", task.getJsonData());
                saveTaskInfo(task);
            } catch (Exception e) {
                log.error("pull data error!", e);
                return;
            }
            try {
                if (  io.ctsi.tenet.framework.TaskStatus.FAILIURE.name().equals(task.getTaskStatus())) {
                    log.debug("drop wrong task");
                    return;
                }
                if (  io.ctsi.tenet.framework.TaskStatus.DROP.name().equals(task.getTaskStatus())) {
                    log.debug("ignore dropped task");
                    return;
                }
                if (  io.ctsi.tenet.framework.TaskStatus.ERROR.name().equals(task.getTaskStatus())) {
                    error(task);
                    return;
                }
                if (config.getMaxRetryTime() <= task.getRetryTime()) {
                    task.setTaskStatus(  io.ctsi.tenet.framework.TaskStatus.FAILIURE.name());
                    saveTaskInfo(task);
                    log.debug("drop task for retry {} times", task.getRetryTime());
                    return;
                }
                Function<TenetMessage, Integer> currentProcess;
                // other status: create、running、retry、success、wait
                if (!task.getTaskStatus().equals(  io.ctsi.tenet.framework.TaskStatus.WAIT.name())) {
                    //非异步任务执行前两个动作
                    task.setTaskStatus(  io.ctsi.tenet.framework.TaskStatus.RUNNING.name());
                    task.setTaskHolder(config.getHolder());
                    saveTaskInfo(task);
                    log.debug("task starting");

                    currentProcess = node.getBefore();
                    //before
                    if (currentProcess != null) {
                        Integer result = currentProcess.apply(task);
                        if (result == null) {
                            log.error("before process return null value,{}", task);
                            error(task);
                            return;
                        }
                        //before可识别的返回状态
                        //SUCCESS,RETRY,DROP,BEFORE_PROCESS_PASS,BEFORE_PROCESS_ERROR
                        switch (result) {
                            case TenetNode.RETRY:
                                log.debug("retry task in before action {}", task);
                                retry(task);
                                return;
                            case TenetNode.DROP:
                                log.debug("drop task in before action {}", task);
                                drop(task);
                                return;
                            case TenetNode.BEFORE_PROCESS_PASS:
                                log.debug("pass task in before action {}", task);
                                clearRetryTimeAndGotoNext(task);
                                return;
                            case TenetNode.BEFORE_PROCESS_ERROR:
                                log.debug("error task in before action {}", task);
                                error(task);
                                return;
                            case TenetNode.SUCCESS:
                                break;
                            default:
                                log.error("错误的返回值！");
                                error(task);
                                return;
                        }
                    }

                    //run
                    currentProcess = node.getRun();
                    if (currentProcess != null) {
                        Integer result = currentProcess.apply(task);
                        if (result == null) {
                            log.error("run process return null value,{}", task);
                            error(task);
                            return;
                        }
                        //run可识别的返回状态
                        //SUCCESS,RETRY,DROP,RUN_PROCESS_ASYNC_WAIT
                        switch (result) {
                            case TenetNode.RETRY:
                                log.debug("retry task in run action {}", task);
                                retry(task);
                                return;
                            case TenetNode.DROP:
                                log.debug("drop task in run action {}", task);
                                drop(task);
                                return;
                            case TenetNode.RUN_PROCESS_ASYNC_WAIT:
                                log.debug("async task in run action {}", task);
                                asyncWait(task);
                                return;
                            case TenetNode.SUCCESS:
                                break;
                            default:
                                log.error("错误的返回值！");
                                error(task);
                                return;
                        }
                    }
                } else {
                    log.debug("async task awake");
                }

                //after
                currentProcess = node.getAfter();
                if (currentProcess != null) {
                    Integer result = currentProcess.apply(task);
                    if (result == null) {
                        log.error("after process return null value,{}", task);
                        error(task);
                        return;
                    }
                    //after可识别的返回状态
                    //SUCCESS,RETRY,DROP
                    switch (result) {
                        case TenetNode.RETRY:
                            log.debug("retry task in run action {}", task);
                            retry(task);
                            return;
                        case TenetNode.DROP:
                            log.debug("drop task in run action {}", task);
                            drop(task);
                            return;
                        case TenetNode.SUCCESS:
                            break;
                        default:
                            log.error("错误的返回值！");
                            error(task);
                            return;
                    }
                }
                clearRetryTimeAndGotoNext(task);
            } catch (Exception e) {
                log.error("task finished with error!", e);
                error(task);
            } finally {
                long n2 = System.nanoTime();
                log.debug("task cost {}ns", n2 - n);
            }
        };
    }

    private void saveTaskInfo(TenetTask tenetTask) {
        log.debug("TODO saving task to mongoDB: {}", tenetTask.getJsonData());
        // TODO save task to database
    }

    private void clearRetryTimeAndGotoNext(TenetTask task) {
        task.setTaskStatus(  io.ctsi.tenet.framework.TaskStatus.SUCCESS.name());
        saveTaskInfo(task);
        log.debug("task success {}", task);
        task.setTaskStatus(  io.ctsi.tenet.framework.TaskStatus.CREATE.name());
        task.setRetryTime(0);
        node.pushNext(task);
        log.debug("task pushed next {}", task);
    }

    private void retry(TenetTask task) {
        task.setRetryTime(task.getRetryTime() + 1);
        task.setTaskStatus(  io.ctsi.tenet.framework.TaskStatus.RETRY.name());
        saveTaskInfo(task);
        node.pushBack(task);
        log.debug("task retry {}", task);
    }

    private void error(TenetTask task) {
        task.setTaskStatus(  io.ctsi.tenet.framework.TaskStatus.ERROR.name());
        saveTaskInfo(task);
        log.debug("task error {}", task);
        if (config.isIgnoreProcessError()) {
            try {
                clearRetryTimeAndGotoNext(task);
            } catch (Exception e) {
                log.error("CAN NOT PUSH NEXT!!!!!", e);
            }
        }
    }

    private void drop(TenetTask task) {
        task.setTaskStatus(  io.ctsi.tenet.framework.TaskStatus.DROP.name());
        saveTaskInfo(task);
        log.debug("task dropped {}", task);
    }

    private void asyncWait(TenetTask task) {
        task.setTaskStatus(  io.ctsi.tenet.framework.TaskStatus.WAIT.name());
        saveTaskInfo(task);
        tenetTaskCache.setCache(task);
        node.getDq().offer(task, config.getMaxAsyncWaiting(), TimeUnit.MILLISECONDS);
        log.debug("task async wait {}", task);
    }
}
