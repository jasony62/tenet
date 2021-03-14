package io.ctsi.tenet.framework.worker;

import lombok.SneakyThrows;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.SmartLifecycle;

import javax.annotation.PostConstruct;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class DaemonTaskManager implements SmartLifecycle, ApplicationContextAware {
    private ApplicationContext applicationContext;
    private ScheduledExecutorService executors;
    private volatile boolean status = false;
    @PostConstruct
    public void init(){
        executors = Executors.newScheduledThreadPool(1,new ThreadFactory (){
            @Override
            public Thread newThread(Runnable r) {
                Thread t=new Thread(r);
                t.setName("tenet-daemon-t0");
                return t;
            }
        });
    }
    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Override
    public void start() {
        synchronized (this) {
            status = true;
            executors.scheduleWithFixedDelay(new DaemonTask(applicationContext), 10, 4, TimeUnit.SECONDS);
        }
    }

    @SneakyThrows
    @Override
    public void stop() {
        synchronized (this) {
            executors.shutdown();
            while (!executors.awaitTermination(1,TimeUnit.SECONDS)) {
            }
            status = false;
        }
    }

    @Override
    public boolean isRunning() {
        return status;
    }

    @Override
    public int getPhase() {
        // 默认为0
        return Integer.MAX_VALUE;
    }

    /**
     * 根据该方法的返回值决定是否执行start方法。<br/>
     * 返回true时start方法会被自动执行，返回false则不会。
     */
    @Override
    public boolean isAutoStartup() {
        // 默认为false
        return true;
    }

}
