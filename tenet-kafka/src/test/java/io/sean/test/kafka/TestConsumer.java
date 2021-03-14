package io.sean.test.kafka;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.junit4.SpringRunner;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@RunWith(SpringRunner.class)
//@SpringBootTest(classes={KafkaProducerConfig.class})
public class TestConsumer {
    private Wrapper wrapper;
    @Before
    public void setUp() throws Exception {
        System.out.println("---init---");
        Class c = Action.class;
        wrapper = new Wrapper();
        wrapper.setM(c.getMethod("doAction",String.class));
        wrapper.setBean(c.newInstance());
    }
    @Test
    public void runMethod() throws Exception {

        ExecutorService e = new ThreadPoolExecutor(5, 5, 1, TimeUnit.MINUTES, new LinkedBlockingQueue<>());

        for(int i=0;i<100;i++) {
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        wrapper.invoke(Thread.currentThread().getName());
                    } catch (InvocationTargetException e) {
                        e.printStackTrace();
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    }
                }
            });
            e.execute(t);

        }

    }

    static class Wrapper{
        private Method m;
        private Object bean;

        public void setM(Method m) {
            this.m = m;
        }

        public void setBean(Object c) {
            this.bean = c;
        }

        public Object invoke(String msg) throws InvocationTargetException, IllegalAccessException {
            return m.invoke(bean,msg);
        }
    }

    static class Action{
        public static final ThreadLocal<Integer> safe = new ThreadLocal<>();
        static{
            safe.set(0);
        }

        public void doAction(String msg){
            int i = 0;
            System.out.println("===msg:"+msg+i);
            safe.set(++i);
            System.out.println("===msg:"+msg+"-"+safe.get());
        }
    }


/*
    @Autowired
    protected ApplicationContext ctx;

    @Before
    public void setUp() throws Exception {
        System.out.println("---init---");
        System.out.println(ctx);
    }

    @Test
    public void sendMessage() {
        final KafkaTemplate<String, String> template = ctx.getBean("kafkaTemplate",KafkaTemplate.class);
        System.out.println("spring test");

        new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i=0;i<1000;i++) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    ListenableFuture<SendResult<String,String>> r = template.send("test_demo", String.valueOf(i), "{\"Event\":\"zxy" + String.valueOf(i)+"\",\"Privilege\":\"a\",\"SequenceNumber\":\"100\"}");
                    System.out.println("done is "+r.isDone());
                }
            }
        }).run();



    }

    //@Test
    public void start() {
          Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test_consumer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "10");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "60000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(props);
        ContainerProperties containerProps = new ContainerProperties("test_demo");
        final CountDownLatch latch = new CountDownLatch(4);
        containerProps.setMessageListener((AcknowledgingMessageListener<Integer, String>) (message, ack) -> {
            System.out.println("manual: " + message);
            ack.acknowledge();
            latch.countDown();
        });

        containerProps.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        ConcurrentMessageListenerContainer<String, String> container =
                new ConcurrentMessageListenerContainer<>(cf, containerProps);
        container.setConcurrency(1);
        container.setBeanName("test" + ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        container.start();
        KafkaListenerEndpointRegistry registry = ctx.getBean( KafkaListenerEndpointRegistry.class);
        if (!registry.getListenerContainer("tenetListener").isRunning()) {
            registry.getListenerContainer("tenetListener").start();
        }
        //项目启动的时候监听容器是未启动状态，而resume是恢复的意思不是启动的意思
        registry.getListenerContainer("tenetListener").resume();
        System.out.println("tenetListener 开启监听成功。");
        System.out.println(Thread.currentThread().getName()+":"+Thread.currentThread());
        System.out.println("===tenet start====");

    }
    @Test
    public void testJdbc() throws SQLException {
        //System.out.println(MessageFormat.format("bbb{0}nnn","aaaaa"));
        Map<String,Object> props = new HashMap<>();
        props.put(JdbcSinkConfig.CONNECTION_URL,"jdbc:mysql://localhost:13306/demo");
        props.put(JdbcSinkConfig.CONNECTION_USER,"root");
        props.put(JdbcSinkConfig.CONNECTION_PASSWORD,"123456");
        props.put(JdbcSinkConfig.TABLE_NAME_FORMAT,"kafka_${topic}");
        props.put(JdbcSinkConfig.BATCH_SIZE,10);
        props.put(JdbcSinkConfig.MAX_RETRIES,10);
        props.put(JdbcSinkConfig.RETRY_BACKOFF_MS,1000);
        props.put(JdbcSinkConfig.AUTO_CREATE,true);
        props.put(JdbcSinkConfig.INSERT_MODE,"UPSERT");
        //props.put(JdbcSinkConfig.PK_FIELDS, Arrays.asList("","",""));
        props.put(JdbcSinkConfig.DB_TIMEZONE_CONFIG,"Asia/Shanghai");
        props.put(JdbcSinkConfig.PK_MODE, "record_key");
        props.put("pk.fields", "id"); // assigned name for the primitive key
        JdbcSinkConfig jdbcSinkConfig = new JdbcSinkConfig(props);
        DbDialect dbDialect =new MySqlDatabaseDialect();
        DbStructure dbStructure = new DbStructure(dbDialect);
        JdbcDbWriter writer = new JdbcDbWriter(jdbcSinkConfig,dbDialect,dbStructure);
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        Schema keySchema = Schema.INT64_SCHEMA;

        Schema valueSchema1 = SchemaBuilder.struct()
                .field("author", Schema.STRING_SCHEMA)
                .field("title", Schema.STRING_SCHEMA)
                .build();

        Struct valueStruct1 = new Struct(valueSchema1)
                .put("author", "Tom Robbins")
                .put("title", "Villa Incognito");

        writer.write(Collections.singleton(new SinkRecord("test_demo", 0, keySchema, 1L, valueSchema1, valueStruct1, 0)));


    }
    //@Test
    public void testConsumerExactlyOnce() {
        System.out.println("---test---");
        System.out.println("--- multi start ----");
        int expectedCount = 50 * 900;
        String brokerId = "localhost:9092";
        String groupId = "test-group";
        String topic = "test_demo";

        long start = 0;
        System.out.println("Multi-threaded consumer costs " + (System.currentTimeMillis() - start));
    }

    //@Test
    public void testAutoCommit() throws Exception {
        //logger.info("Start auto");
        System.out.println("Start auto");
        ContainerProperties containerProps = new ContainerProperties("test_demo");
        final CountDownLatch latch = new CountDownLatch(4);
        containerProps.setMessageListener(new MessageListener<Integer, String>() {

            @Override
            public void onMessage(ConsumerRecord<Integer, String> message) {
                System.out.println("received: " + message);
                latch.countDown();
            }

        });
        KafkaMessageListenerContainer<Integer, String> container = createContainer(containerProps);
        container.setBeanName("testAuto");
        container.start();
        Thread.sleep(1000); // wait a bit for the container to start
        KafkaTemplate<Integer, String> template = createTemplate();
        template.setDefaultTopic("test_demo");
        template.sendDefault(0, "foo");
        template.sendDefault(2, "bar");
        template.sendDefault(0, "baz");
        template.sendDefault(2, "qux");
        template.flush();
        assertTrue(latch.await(60, TimeUnit.SECONDS));
        container.stop();
        System.out.println("Stop auto");

    }

    private KafkaMessageListenerContainer<Integer, String> createContainer(
            ContainerProperties containerProps) {
        Map<String, Object> props = consumerProps();
        DefaultKafkaConsumerFactory<Integer, String> cf =
                new DefaultKafkaConsumerFactory<Integer, String>(props);
        KafkaMessageListenerContainer<Integer, String> container =
                new KafkaMessageListenerContainer<>(cf, containerProps);
        return container;
    }

    private KafkaTemplate<Integer, String> createTemplate() {
        Map<String, Object> senderProps = senderProps();
        ProducerFactory<Integer, String> pf =
                new DefaultKafkaProducerFactory<Integer, String>(senderProps);
        KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
        return template;
    }

    private Map<String, Object> consumerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test_consumer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return props;
    }

    private Map<String, Object> senderProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }*/
//{"Event":"zxy"}
}
