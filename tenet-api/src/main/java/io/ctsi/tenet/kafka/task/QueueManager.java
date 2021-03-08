package io.ctsi.tenet.kafka.task;

import io.ctsi.tenet.kafka.connect.sink.InternalSinkRecord;
import io.ctsi.tenet.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class QueueManager  {

    private static final Logger logger = LoggerFactory.getLogger(QueueManager.class);

    //初始化11100000000000000000000000000000
    private final AtomicInteger control = new AtomicInteger(0);

    private static final int COUNT_BITS = Short.SIZE - 1;
    private ReentrantLock mainLock;
    private BlockingQueue<SinkRecord> msgQueue;
    //private static final int WRITE_ENABLE  = 0 ;

    private volatile boolean  addResult = true;

    private static int CAPACITY = (1 << COUNT_BITS) - 1;

    public void init(int maxSize) {
        //System.out.println("====CAPACITY===="+CAPACITY);
        if (maxSize > CAPACITY)
            throw new Error("TenetQueue is so long");

        CAPACITY = maxSize;
        this.msgQueue = new LinkedBlockingQueue<>(CAPACITY);
        mainLock = new ReentrantLock();

       // doTest(this);
    }

    public boolean addMessage(List<InternalSinkRecord> list) {

        int addSize = list.size();
        logger.info("--- addMessage get recordList addSize is {}", addSize);
        if (list.isEmpty()) {
            return true;
        }
        int controlNum = control.get();
        logger.info("--- addMessage controlNum is {},CAPACITY is {}", controlNum, CAPACITY);
        if (controlNum >= CAPACITY || (controlNum+addSize) >CAPACITY) {
            addResult = false;
            return false;
        }

        // 必要检查锁，防止atomic自旋锁导致kafka poll超时,减少自旋锁
        logger.info("--- addMessage get lock queue {} ,control is {}", this.msgQueue.size(), controlNum + addSize);
        if (!mainLock.tryLock()) {
            return false;
        }

        boolean addMsg = false;
        logger.info("--- addMessage get lock success queue size is {}", this.msgQueue.size());
        try {
            controlNum = control.get();
            logger.info("--- double check addMessage prepare update controlNum is {}", controlNum+addSize);
            if ((controlNum+addSize) <= CAPACITY) {
                this.msgQueue.addAll(list);
                do{
                    // 保证成功
                } while(!this.control.compareAndSet(controlNum, controlNum + addSize));
                addMsg = true;
            } else {
                logger.info("--- addMessage after controlNum is {},but target num is to large {}", controlNum, controlNum - addSize);
            }
        } finally {
            logger.info("--- addMessage release lock");
            mainLock.unlock();
            addResult = addMsg;
        }
        logger.info("--- addMessage result is {},this queue size is {} ", addMsg, this.msgQueue.size());
        return addMsg;

    }
    @Deprecated
    public SinkRecord pullData(int keepAliveTime, TimeUnit timeUnit) {
        /*int ctrl = this.control.get();
        logger.info("--- before pullData queue {} ,control is {}", this.msgQueue.size(), ctrl);
        if (ctrl == 0) {
            //control.compareAndSet(ctrl, 0);
            return null;
        }
        logger.trace("--- pullData get lock tryLock");
        if (!mainLock.tryLock()) {
            return null;
        }
        logger.trace("--- pullData get lock tryLock success");
        try {
            ctrl = this.control.decrementAndGet();
            logger.trace("--- pullData get lock ,control update success is {}", ctrl);
        } finally {
            logger.trace("--- pullData release lock");
            mainLock.unlock();
        }
        try {
            return keepAliveTime > 0 ?
                    msgQueue.poll(keepAliveTime, timeUnit) :
                    msgQueue.take();
        } catch (InterruptedException e) {
            logger.trace("--- pullData await Queue InterruptedException");
            return  null;
        }*/
        return null;

    }

    public SinkRecord pullData(boolean timed ,long keepAliveTime, TimeUnit timeUnit) throws InterruptedException {

        SinkRecord s = null;

        logger.info("--- before pullData queue {} ,control is {}" ,this.msgQueue.size() , this.control.get());
        s = timed ?
                msgQueue.poll(keepAliveTime, timeUnit) :
                msgQueue.take();
        logger.info("--- ing pullData queue {} ,s is {}",this.msgQueue.size() , s==null);
        if(s != null){
            try {
                mainLock.lock();
                this.control.getAndDecrement();
            }finally{
                mainLock.unlock();
                logger.info("---after pullData queue {} ,control is {}",this.msgQueue.size() , this.control.get());
            }
        }
        return s;
    }

    public int getCtrol(){
        return this.control.get();
    }

    public boolean getAddResult() {
        return addResult;
    }

    public BlockingQueue<SinkRecord> getMsgQueue() {
        return msgQueue;
    }

    public void decrementAndGet(){
        this.control.decrementAndGet();
    }

    /*private boolean compareAndIncrementWorkerCount(int expect, int inc) {
        return control.compareAndSet(expect, expect + inc);
    }

    private void decrementWorkerCount(int expect,int target) {
        do {
        } while (!compareAndDecrementWorkerCount(expect,target));
    }

    private boolean compareAndDecrementWorkerCount(int expect,int target) {
        return control.compareAndSet(expect, expect-target);
    }

    public AtomicInteger getControl() {
        return control;
    }

    public void doTest(final QueueManager qm) {
        //
        // final BufferManager bm = applicationContext.getBean(TenetWorkerTaskConfigUtils.TENET_MESSAGE_BUFFER_BEAN_NAME,BufferManager.class);
        Map<String,Object> map = new HashMap<>();
        map.put("acks","all");
        map.put("batch.size",10240);
        map.put("bootstrap.servers","localhost:9092");
        map.put("key.serializer","org.apache.kafka.common.serialization.ByteArraySerializer");
        map.put("value.serializer","org.apache.kafka.common.serialization.ByteArraySerializer");
        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<byte[], byte[]>(map);
        final String str = "{\"Event\":\"Cdr\",\"Privilege\":\"cdr,all\",\"SequenceNumber\":\"%s\",\"File\":\"cdr_manager.c\",\"Line\":\"359\",\"Func\":\"manager_log\",\"SystemName\":\"192.168.20.85\",\"AccountCode\":\"aaa\",\"Source\":\"17710362283\",\"Destination\":\"17710362283\",\"DestinationContext\":\"out-context\",\"Channel\":\"PJSIP/incoming-trunk-00000000\",\"DestinationChannel\":\"PJSIP/incoming-trunk-00000001\",\"LastApplication\":\"Dial\",\"LastData\":\"PJSIP/incoming-trunk/sip:01066668888@192.168.20.91,,b(handler^addheader^1)\",\"StartTime\":\"2021-01-04 09:22:45\",\"AnswerTime\":\"2021-01-04 09:22:45\",\"EndTime\":\"2021-01-04 09:23:01\",\"Duration\":\"12\",\"BillableSeconds\":\"10\","
                + "\"Disposition\":\"ANSWERED\",\"AMAFlags\":\"DOCUMENTATION\",\"UniqueID\":\"192.168.20.85-1605057769.0\",\"UserField\":\"19910000002$19910780003$19910000001$1$yly$beijing$8500092319860520201111092249581437777871$2021-01-04 09:22:49$1$19910780001$\",\"chan_hangup\":{\"Cause\":\"31\",\"Cause_txt\":\"Normal, unspecified\"},\"dstchan_hangup\":{\"Cause\":\"100\",\"Cause_txt\":\"Normal, unspecified\"}}";

        for(int i = 0 ;i<6;i++){
            Thread t1 = new Thread(new Runnable() {

                @Override
                public void run() {
                    try {
                        for (int j = 0; j <300 ; j++) {
                            logger.debug("------------+++++++++++++doTest1");
                            Thread.currentThread().sleep(100);
                            ProducerRecord record = new ProducerRecord<byte[],byte[]>("test_demo",str.getBytes());
                            producer.send(record);
//                        SinkRecord r = qm.pullData(2, TimeUnit.SECONDS);
//
//                        if (r != null) {
//                            bm.addMessage("{\"name\":\"zxy1\",\"SequenceNumber\":\"24\"}}");
//                            logger.debug("-------++++++++++++r1:{}", r.value());
//                        }else
//                            logger.debug("--++++++++++++++++++++r1:{} nothing");
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
            t1.start();
        }

        Thread t2 = new Thread(new Runnable() {

            @Override
            public void run() {

                logger.debug("------------+++++++++++++doTest2");
                try {
                    for (int i = 0; i < 1000; i++) {
                        logger.debug("------------+++++++++++++doTest1");
                        Thread.currentThread().sleep(500);
                        String r = String.format(str,i+"");
                        ProducerRecord record = new ProducerRecord<byte[],byte[]>("test_demo",r.getBytes());
                        producer.send(record);
                    }
                }catch(InterruptedException e) {
                    e.printStackTrace();
                }
//                        SinkRecord r = qm.pullData(2, TimeUnit.SECONDS);
//                        //bm.addMessage(r.value().toString());
//                        if (r != null){
//                            bm.addMessage("{\"name\":\"zxy2\",\"SequenceNumber\":\"24\"}");
//                            logger.debug("-------++++++++++++r1:{}", r.value());
//                        }else
//                            logger.debug("--++++++++++++++++++++r2:{} nothing");


            }

        });

        t2.start();
    }



    private ApplicationContext applicationContext;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }*/
}
