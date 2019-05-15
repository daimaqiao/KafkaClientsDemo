package dmq.test.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;


// 数据接收端示例：自动从“SimpleDemo”拉取发送当时时间信息
public class SimpleConsumer {
    private final String servers, topic;
    private Consumer<String, String> consumer= null;
    private boolean subscribeOnce= true;
    public SimpleConsumer(String servers, String topic) {
        this.servers= servers;
        this.topic= topic;
    }

    public void open(Properties custom) {
        if(consumer != null)
            return;
        // 默认配置只是为了展示常用配置项
        Properties properties= consumerProperties(servers);
        // 添入自定义配置
        if(custom != null)
            properties.putAll(custom);
        consumer= new KafkaConsumer<>(properties);
    }
    public void close() {
        if(consumer == null)
            return;
        consumer.close();
        consumer= null;
    }

    public ConsumerRecords<String, String> poll(long timeout) {
        if(consumer == null)
            return null;
        if(subscribeOnce) {
            subscribeOnce= false;
            consumer.subscribe(Arrays.asList(topic));
        }
        return consumer.poll(timeout);
    }

    // 默认配置文件
    private Properties consumerProperties(String servers) {
        Properties properties= new Properties();
        // Kafka服务器列表，“HOST:PORT”形式，多个服务器地址间用逗号分隔
        properties.put("bootstrap.servers", servers);
        // 最低拉取字节数（默认1）
        properties.put("fetch.min.bytes", 1);
        // 设置消费者分组，同组消费者共享同一个offset，否则组名必须唯一（默认空）
        properties.put("group.id", String.format("%s-%s", this.getClass().getSimpleName(), this.toString()));
        // 心跳周期，必须小于会话超时时间（默认3000， < session.timeout.ms）
        properties.put("heartbeat.interval.ms", 3000);
        // 单次最大拉取每分区字节数，小于消息最大字节数（默认1048576， < MIN(message.max.bytes, max.message.bytes)）
        properties.put("max.partition.fetch.bytes", 1048576);
        // 会话超时时间，必须介于最小组内会话超时，最大组内会话超时之间
        // （默认10000， BETWEEN(group.min.session.timeout.ms, group.max.session.timeout.ms)）
        properties.put("session.timeout.ms", 10000);
        // 自动配置缺失的offset，可选值：earliest=最早的，latest=最近的（默认），none=抛出异常
        properties.put("auto.offset.reset", "latest");
        // 最大空闲时间，自动关闭空闲的连接（默认540000）
        properties.put("connections.max.idle.ms", 540000);
        // 自动提交offset（默认true）
        properties.put("enable.auto.commit", true);
        // 是否屏蔽内部topics，如offsets等（默认true）
        properties.put("exclude.internal.topics", true);
        // 单次最大拉取字节数，小于消息最大字节数（默认52428800， < MIN(message.max.bytes, max.message.bytes)）
        properties.put("fetch.max.bytes", 52428800);
        // 最大拉取间隔，超过这个间隔仍获取不到数据则认为失败，并将对应分区分配给其他组，（默认300000，最小1）
        properties.put("max.poll.interval.ms", 300000);
        // 单次最大拉取记录数（默认500，最小1）
        properties.put("max.poll.records", 500);
        // 请求超时时间，请求之后未收到回复，会重新发出请求，多次重试无效则失败（默认305000）
        properties.put("request.timeout.ms", 305000);
        // 自动提交offset的周期（默认5000）
        properties.put("auto.commit.interval.ms", 5000);
        // 检查CRC32，性能受损严重时可关闭（默认true）
        properties.put("check.crcs", true);
        // 客户端ID（默认空）
        properties.put("client.id", this.getClass().getSimpleName());
        // 最大拉取时阻塞，poll的传入参数不可大于此值（默认500）
        properties.put("fetch.max.wait.ms", 500);
        // 服务器信息最大有效期（默认300000）
        properties.put("metadata.max.age.ms", 300000);
        // key/value序列化类
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return properties;
    }


    public static void main(String[] args) {
        final String TOPIC= "KafkaClientsDemo";
        final String SERVERS= "localhost:9092";

        // 1. 创建
        SimpleConsumer simpleConsumer= new SimpleConsumer(SERVERS, TOPIC);

        System.out.printf("this= %s\n", simpleConsumer.toString());
        System.out.printf("name= %s\n", simpleConsumer.getClass().getName());
        System.out.printf("typeName= %s\n", simpleConsumer.getClass().getTypeName());
        System.out.printf("canonicalName= %s\n", simpleConsumer.getClass().getCanonicalName());

        // 加入对语句间的时间对比
        TimeComparer tc= new TimeComparer(simpleConsumer.getClass().getSimpleName());
        tc.reset("new consumer");

        // 2. 配置并打开
        Properties properties= new Properties();
        properties.put("bootstrap.servers", SERVERS);
        properties.put("group.id", simpleConsumer.toString());
        properties.put("client.id", simpleConsumer.getClass().getSimpleName());
        properties.put("auto.offset.reset", "latest");
        properties.put("fetch.max.wait.ms", 2000);
        properties.put("auto.commit.interval.ms", 2000);
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        simpleConsumer.open(properties);

        tc.update();
        System.out.println("READY ...");
        try {
            System.out.println("GO!");
            while (true) {
                // 默认配置允许自动创建不存在的topic
                tc.reset("consumer.poll");
                System.out.println("Polling ...");
                ConsumerRecords<String, String> records=

                        // 3. 收取
                        simpleConsumer.poll(2000);

                tc.update();
                int count= records == null? -1: records.count();
                System.out.println();
                System.out.printf("Check records ... (count= %d)\n", count);
                if(count <= 0)
                    continue;

                records.forEach(x-> {
                    System.out.printf("%s -- %s\n", x.key(), x.value());
                    System.out.printf("offset=%d, partition=%d\n", x.offset(), x.partition());
                });
            }// while
        } catch (Exception x) {
            x.printStackTrace();
        } finally {
            System.out.println("DONE!");

            // 4. 关闭
            simpleConsumer.close();
        }// try/catch/finally
    }// main
}// class
