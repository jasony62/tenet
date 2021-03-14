package io.ctsi.tenet.framework.worker;

import io.ctsi.tenet.kafka.connect.AbstractStatus;
import io.ctsi.tenet.kafka.connect.ConnectorStatus;
import io.ctsi.tenet.kafka.connect.sink.TaskStatus;
import io.ctsi.tenet.kafka.connect.sink.WorkerTask;
import io.ctsi.tenet.kafka.connect.storage.MemoryStatusBackingStore;
import io.ctsi.tenet.kafka.task.util.TenetWorkerTaskConfigUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;

@Slf4j
public class DaemonTask implements Runnable{

    private ApplicationContext context;
    MemoryStatusBackingStore memoryStatusBackingStore;
    TenetWorkerLifecycle tenetWorkerLifecycle;
    public DaemonTask(ApplicationContext context){
        this.context = context;
        tenetWorkerLifecycle = context.getBean(TenetWorkerLifecycle.class);
        memoryStatusBackingStore = context.getBean(TenetWorkerTaskConfigUtil.DEFAULT_MEMORY_STATUS_BACKING_STORE_BEAN_NAME,MemoryStatusBackingStore.class);
    }

    @Override
    public void run() {
        try {
            checkTenetStatus();
        }catch(Throwable t){
            log.error("--- daemon thread error:",t);
        }
    }

    /***
     * TODO
     * @return
     */
    public void checkTenetStatus(){

        log.info("======== check tenet lifecycle status {} ========",tenetWorkerLifecycle.isRunning());

        if(tenetWorkerLifecycle.isRunning()){
            tenetWorkerLifecycle.getTaskContainer().forEach((k,v)->{
                log.info("========  check ========"+k.toString());
                TaskStatus ts = memoryStatusBackingStore.get(k);
                if(ts.state() == AbstractStatus.State.FAILED){
                    tenetWorkerLifecycle.stop();
                    log.error("======== check ======== {} failed shutdown tenet",k.toString()) ;
                } else{
                    log.info("======== check {} {} ======== ",k.toString(),ts.state()) ;
                }
            });

        }

    }

}
