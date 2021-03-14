package io.ctsi.tenet.framework;

public enum TaskStatus {
    /**
     * 待处理
     */
    CREATE,
    /**
     * 处理中
     */
    RUNNING,
    /**
     * 待重试
     */
    RETRY,
    /**
     * 处理成功
     */
    SUCCESS,
    /**
     * 处理错误数据，直接丢弃
     */
    ERROR,
    /**
     * 处理失败
     */
    FAILIURE,
    /**
     * 异步丢弃
     */
    DROP,
    /**
     * 等待异步结果
     */
    WAIT
}
