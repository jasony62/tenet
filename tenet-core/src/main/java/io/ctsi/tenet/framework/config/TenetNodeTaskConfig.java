package io.ctsi.tenet.framework.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author Mc.D
 */
@ConfigurationProperties(prefix = "tenet.task")
@Getter
@Setter
public class TenetNodeTaskConfig {
    /**
     * 任务持有人（服务名称）
     */
    private String holder = "default-task-holder";
    /**
     * 最大重试次数
     */
    private int maxRetryTime = 3;

    /**
     * 重试间隔（毫秒数）
     */
    private long retryDelay = 300;

    /**
     * 最大异步等待时间（毫秒数，为0时无限期等待）
     */
    private long maxAsyncWaiting = 2000;

    /**
     * 是否略处理错误，如忽略处理错误，则出错的任务会继续发送到下一个节点
     */
    private boolean ignoreProcessError = false;
}
