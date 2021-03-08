package io.ctsi.tenet.framework;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * @author Mc.D
 */
@Getter
@Setter(AccessLevel.PACKAGE)
public class TenetTask extends TenetMessage implements Serializable {

    private static final long serialVersionUID = -6096297243221096693L;

    /**
     * 任务唯一编号
     */
    private String taskId;
    /**
     * 任务状态
     */
    private String taskStatus;
    /**
     * 重试次数
     */
    private int retryTime;
    /**
     * 任务持有人（服务名称）
     */
    private String taskHolder;
}
