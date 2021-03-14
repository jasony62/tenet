package io.ctsi.tenet.kafka.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class BufferManager {

    private static final Logger logger = LoggerFactory.getLogger(BufferManager.class);
    private static final String SEND_RESULT = "send_result";
    private final AtomicInteger control = new AtomicInteger(0);

    //private ArrayList<ConsumerRecord<byte[], byte[]>> msgBatch;
    private ArrayList<String> msgBatch;
    private ReentrantLock reentrantLock;
    private Condition waitFullLock;
    private Condition waitEmptyLock;
    private static int CAPACITY = Short.MAX_VALUE;
    //private KafkaProducer<byte[], byte[]> producer;
    //private String topic;
    //private Converter valueConverter;
    //private ObjectMapper mapper = new ObjectMapper();

    //public void init(int maxSize, KafkaProducer<byte[], byte[]> producer, String topic, Converter converter) {
    public void init(int maxSize) {
        if (maxSize > CAPACITY)
            throw new IllegalArgumentException("--Queue is so long ");
        CAPACITY = maxSize;
        this.msgBatch = new ArrayList<>(CAPACITY);
        this.reentrantLock = new ReentrantLock();
        this.waitFullLock = this.reentrantLock.newCondition();
        this.waitEmptyLock = this.reentrantLock.newCondition();
        //this.producer = producer;
        //this.topic = topic;
        //this.valueConverter = converter;
    }

    /**
     * 业务线程添加数据到输出mongo的线程，类似队列
     * @param sourceRecord
     * @return
     */
    public boolean addMessage(String sourceRecord) {
        logger.info("--- addMessage send CAPACITY is {},MSG is {}", CAPACITY,sourceRecord);
        //ConsumerRecord<byte[], byte[]>   record = sendMessage(sourceRecord);

        this.reentrantLock.lock();
        logger.info("--- bufferMessage thread is get lock success {}", Thread.currentThread().getName());
        Boolean addMsg = false;
        try{
            int controlNum = control.get();
            logger.info("--- bufferMessage before compare controlNum is {},CAPACITY is {}", controlNum, CAPACITY);
            if (controlNum >= CAPACITY) {
                //logger.debug("--addMessage thread is prepared await {}", Thread.currentThread().getName());
                try {
                    /**
                     * 当signalALl所有线程被唤醒，防止越界，自旋锁解决
                     * */
                    for(;;) {
                        this.waitFullLock.await(1, TimeUnit.SECONDS);
                        if(control.addAndGet(1)<=CAPACITY){
                            break;
                        }
                        control.decrementAndGet();
                    }
                    controlNum = this.control.get();
                    logger.info("--- bufferMessage before add controlNum is {}", controlNum);
                    addMsg = true;
                } catch (Throwable t) {
                    this.reentrantLock.unlock();
                    logger.error("--- bufferMessage await interrupt t {},msg is {}", t,sourceRecord);
                }
                //logger.debug("--addMessage thread is finish await {}", Thread.currentThread().getName());
            }else{
                addMsg = true;
                controlNum = this.control.incrementAndGet();
                logger.info("--- bufferMessage before add controlNum is {}", controlNum);
            }

            if(addMsg) {
                this.msgBatch.add(sourceRecord);
                controlNum = this.control.incrementAndGet();
                logger.trace("--- addMessage after add controlNum is {}", controlNum);
                //if (controlNum >= CAPACITY / 2)
                this.waitEmptyLock.signal();
            }

        }finally {
            this.reentrantLock.unlock();
        }

        //ConsumerRecord<byte[],byte[]> record = new ConsumerRecord<byte[],byte[]>(this.topic, null, null);
        return addMsg;

    }

    /*
     * fetch 完清除原有集合
     */
    //public List<ConsumerRecord<byte[], byte[]>> fetchData() {
    public List<String> fetchData() {

        int currentStatus = control.get();
        logger.info("--- fetchData  from list  size  is {}", currentStatus);
        if (currentStatus == 0) {
            this.reentrantLock.lock();
            logger.trace("--- main thread is prepared await {}", Thread.currentThread().getName());
            try {
                waitEmptyLock.await(1, TimeUnit.SECONDS);
            } catch (Throwable t) {
                logger.error("--fetchData await interrupt t {}", t);
            } finally {
                this.reentrantLock.unlock();
            }
            //logger.debug("--main thread is finish await {}", Thread.currentThread().getName());
        }
        //ArrayList<ConsumerRecord<byte[], byte[]>> list = this.msgBatch;
        ArrayList<String> list = this.msgBatch;
        this.reentrantLock.lock();
        logger.trace("--- main thread is finish await get lock success {}", Thread.currentThread().getName());
        try {
            int controlNum = control.get();
            this.msgBatch = new ArrayList<>(CAPACITY);
            compareAndDecrementWorkerCount(controlNum, 0);
            //if (controlNum >= CAPACITY/2 ) {
            logger.trace("--- fetchData notice await thread current buffer number is {}", controlNum);
            this.waitFullLock.signalAll();
            //}
        } finally {
            this.reentrantLock.unlock();
        }
        return list;

    }

    private boolean compareAndDecrementWorkerCount(int expect, int target) {
            do{ }while(!control.compareAndSet(expect, target));
            return true;
    }

    /*private ConsumerRecord<byte[], byte[]> sendMessage(String sourceRecord) {
        RecordMetadata recordMetadata = null;
        JsonNode jsonNode = null;
        try {
            jsonNode = mapper.readTree(sourceRecord);
        } catch (JsonProcessingException e) {
            logger.error("tenet result is not json error:{}", e);
            throw new RuntimeException("tenet's result is not json error");
        }
        ProducerRecord<byte[], byte[]> record = convertTransformedRecord(jsonNode);
        try {
            recordMetadata = producer.send(record).get();
        } catch (InterruptedException e) {
            logger.error("tenet kafka producer send is error:{}", e);
        } catch (ExecutionException e) {
            logger.error("tenet kafka producer send is error:{}", e);
        }

        return transToConsumerRecord(recordMetadata, jsonNode);
        //return transToConsumerRecord(recordMetadata,record);
    }

    private ConsumerRecord<byte[], byte[]> transToConsumerRecord(RecordMetadata recordMetadata, JsonNode jsonNode) {

        String topic = recordMetadata.topic();//jsonNode.get("topic").asText();
        int partition = recordMetadata.partition();
        long offset = recordMetadata.offset();
        ObjectNode objectNode = JsonNodeFactory.instance.objectNode();
        Iterator<Map.Entry<String, JsonNode>> it = jsonNode.fields();
        Map.Entry<String, JsonNode> tmp;
        while (it.hasNext()) {
            tmp = it.next();
            objectNode.set(tmp.getKey(), tmp.getValue());
        }
        if (recordMetadata == null) {
            //send error save
            objectNode.put(SEND_RESULT, false);
        } else {
            objectNode.put(SEND_RESULT, true);
        }
        ConsumerRecord<byte[], byte[]> consumerRecord = null;
        try {
            consumerRecord = new ConsumerRecord<byte[], byte[]>(topic, partition, offset, null, mapper.writeValueAsBytes(objectNode));
        } catch (IOException e) {
            logger.error("tenet kafka producer send result trans byte error:{}", e);
        }
        return consumerRecord;

    }

    private ProducerRecord<byte[], byte[]> convertTransformedRecord(JsonNode record) {

        if (record == null) {
            return null;
        }
        RetryWithToleranceOperator retryWithToleranceOperator = new RetryWithToleranceOperator(3, 5, ToleranceType.ALL, new SystemTime());
        byte[] value = retryWithToleranceOperator.execute(() -> valueConverter.fromConnectData(this.topic, null, null, record),
                Stage.VALUE_CONVERTER, valueConverter.getClass());

        if (retryWithToleranceOperator.failed()) {
            return null;
        }
        return new ProducerRecord<>(this.topic, value);

    }*/

    public int getCtrl(){
        return this.control.get();
    }

}
