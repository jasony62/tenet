package io.ctsi.tenet.framework;

import java.util.function.Function;

/**
 * @author Mc.D
 */
public interface TenetNode {

      io.ctsi.tenet.framework.TenetTaskDelayQueue getDq();

    /**
     * 流程结果：处理正常，继续后续处理
     */
    int SUCCESS = 0;

    /**
     * 流程结果：处理有问题，进入重试（每次重试从头处理整个任务）
     */
    int RETRY = -1;

    /**
     * 流程结果：丢弃任务，不送到下一节点
     */
    int DROP = 1;

    /**
     * 预处理流程结果：不处理该任务，直接送到下一节点
     */
    int BEFORE_PROCESS_PASS = 10;

    /**
     * 预处理流程结果：错误的任务，不进行处理，根据配置选择是否送到下一节点
     */
    int BEFORE_PROCESS_ERROR = 11;

    /**
     * 处理流程结果：异步等待
     */
    int RUN_PROCESS_ASYNC_WAIT = 20;


    /**
     * 获取消息信息
     *
     * @return Tenet消息
     */
    TenetTask pullData();

    /**
     * 处理失败的消息回传
     *
     * @param task 处理失败的消息
     */
    void pushBack(TenetTask task);

    /**
     * 处理成功的消息提交，转发给下一级处理机制
     *
     * @param task 处理成功的消息
     */
    void pushNext(TenetTask task);

    /**
     * 预处理TenetTask
     *
     * @return 处理函数
     */
    Function<TenetMessage, Integer> getBefore();

    /**
     * 处理TenetTask
     *
     * @return 处理函数
     */
    Function<TenetMessage, Integer> getRun();

    /**
     * 处理TenetTask后执行的方法
     *
     * @return 处理函数
     */
    Function<TenetMessage, Integer> getAfter();
}
