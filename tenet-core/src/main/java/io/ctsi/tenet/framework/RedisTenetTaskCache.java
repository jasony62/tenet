package io.ctsi.tenet.framework;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;

/**
 * @author Mc.D
 */
@Slf4j
public class RedisTenetTaskCache implements   io.ctsi.tenet.framework.TenetTaskCache {

    private final RedisTemplate<String, String> rt;

    private final Gson gson = new Gson();

    public RedisTenetTaskCache(RedisTemplate<String, String> rt) {
        this.rt = rt;
    }

    @Override
    public TenetTask getTask(String taskId) {
        try {
            String json = rt.opsForValue().get(taskId);
            rt.delete(taskId);
            return json == null ? null : gson.fromJson(json, TenetTask.class);
        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }

    @Override
    public void setCache(TenetTask tenetTask) {
        rt.opsForValue().set(tenetTask.getTaskId(), gson.toJson(tenetTask));
    }
}
