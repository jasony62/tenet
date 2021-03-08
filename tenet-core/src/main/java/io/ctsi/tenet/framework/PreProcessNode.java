package io.ctsi.tenet.framework;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;

import java.util.UUID;
import java.util.function.Function;

/**
 * 初始节点任务工厂
 *
 * @author Mc.D
 */

@Slf4j
public class PreProcessNode extends  io.ctsi.tenet.framework.AbstractKafkaTenetNode {
    private final Gson gson = new Gson();

    @Override
    public Function<TenetMessage, Integer> getBefore() {
        return null;
    }

    @Override
    public Function<TenetMessage, Integer> getRun() {
        return tenetMessage -> {
            // 如果拿到的消息已经有唯一值，直接返回处理成功。
            // TenetTask task = gson.fromJson(tenetMessage.getJsonData(), TenetTask.class);
            if (tenetMessage instanceof TenetTask) {
                TenetTask tenetTask = (TenetTask) tenetMessage;
                if (tenetTask.getTaskId() != null) {
                    log.debug("pass the task {}", tenetTask.getTaskId());
                    return SUCCESS;
                }
                // 如果拿到的消息没有唯一值，则生成一个唯一值
                tenetTask.setTaskId(UUID.randomUUID().toString());
                log.debug("gen new task id for {}", tenetTask.getTaskId());
            }

            return SUCCESS;
        };
    }

    @Override
    public Function<TenetMessage, Integer> getAfter() {
        return null;
    }
}
