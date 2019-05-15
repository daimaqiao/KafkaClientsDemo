package dmq.test.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.Future;


// 数据发送端示例：向“SimpleDemo”发送当前时间信息
public class SimpleProducer {
    private final String servers, topic;
    private Producer<String, String> producer= null;
    public SimpleProducer(String servers, String topic) {
        this.servers= servers;
        this.topic= topic;
    }

    public void open(Properties custom) {
        if(producer != null)
            return;
        // 默认配置只是为了展示常用配置项
        Properties properties= producerProperties(servers);
        // 添入自定义配置
        if(custom != null)
            properties.putAll(custom);
        producer= new KafkaProducer<>(properties);
    }
    public void close() {
        if(producer == null)
            return;
        producer.close();
        producer= null;
    }

    public Future<RecordMetadata> send(String key, String value) {
        if(producer == null)
            return null;

        ProducerRecord<String, String> record= new ProducerRecord<>(topic, key, value);
        return producer.send(record);
    }

    // 默认配置文件
    private Properties producerProperties(String servers) {
        Properties properties= new Properties();
        // Kafka服务器列表，“HOST:PORT”形式，多个服务器地址间用逗号分隔
        properties.put("bootstrap.servers", servers);
        // 设置是否等待写入确认（字符串值）：0=不等待确认，1=仅leader确认（默认，不管follower），
        // -1=all=在leader等待1个以上follower确认之后才能确认成功
        properties.put("acks", "1");
        // 数据压缩选项，batching越大效果越好，可选：none（默认）, gzip, snappy, lz4
        properties.put("compression.type", "none");
        // 有效缓存空间，主要用于发送缓存，压缩缓存等，超出此限制会抛出异常（默认33554432）
        properties.put("buffer.memory", 33554432);
        // 最大尝试次数，写入失败时尝试重新写入（默认0，最大2147483647，无acks时失效）
        properties.put("retries", 0);
        // 发送记录前等待的最大缓存量（默认16384）
        properties.put("batch.size", 16384);
        // 客户端ID（默认空）
        properties.put("client.id", this.getClass().getSimpleName());
        // 最大空闲时间，自动关闭空闲的连接（默认540000）
        properties.put("connections.max.idle.ms", 540000);
        // 最长等待时间，当未达到batching大小时等待时长（默认0）
        properties.put("linger.ms", 0);
        // 最大阻塞时间，当由于kafka原因造成send不能立刻返回时（默认60000）
        // 包含首次（或必要时）连接kafka的时候，获取服务器参数的时间，
        // 或者发生在kafka内部缓存不足时，重新分配缓存空间的时间
        properties.put("max.block.ms", 60000);
        // 最大单次发送数据大小，为避免一次性传输大尺寸数据而自动拆分，（默认1048576）
        properties.put("max.request.size", 1048576);
        // 等待确认超时时间，由于kafka原因，等待acks的超时时间（默认30000）
        properties.put("timeout.ms", 30000);
        // 最大待确认请求数，同一个连接中，允许多个请求同时发生，而不会阻塞（默认5，最小为1）
        properties.put("max.in.flight.requests.per.connection", 5);
        // 获取服务器参数超时时间，首次（或必要时）连接kafka的过程中，需要同时获取服务器超时时间（默认60000）
        properties.put("metadata.fetch.timeout.ms", 60000);
        // 服务器参数信息更新周期（默认300000）
        properties.put("metadata.max.age.ms", 300000);
        // key/value序列化类
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return properties;
    }


    public static void main(String[] args) {
        final String TOPIC= "KafkaClientsDemo";
        final String SERVERS= "localhost:9092";

        // 1. 创建
        SimpleProducer simpleProducer= new SimpleProducer(SERVERS, TOPIC);

        // 加入对语句间的时间对比
        TimeComparer tc= new TimeComparer(simpleProducer.getClass().getSimpleName());
        tc.reset("new producer");

        // 2. 配置并打开
        Properties properties= new Properties();
        properties.put("bootstrap.servers", SERVERS);
        properties.put("client.id", simpleProducer.getClass().getSimpleName());
        properties.put("batch.size", 4096);
        properties.put("linger.ms", 0);
        properties.put("max.block.ms", 1000);
        properties.put("metadata.fetch.timeout.ms", 5000);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        simpleProducer.open(properties);

        tc.update();
        System.out.println("READY ...");
        try {
            System.out.println("GO!");
            long count= 1;
            while (true) {
                Date now= new Date();
                String key= Long.toString(count);
                String value= now.toString();
                System.out.printf("%s -- %s%n", key, value);
                // 默认配置允许自动创建不存在的topic
                tc.reset("producer.send");
                Future<RecordMetadata> future =

                        // 3. 发送
                        simpleProducer.send(key, value);

                tc.update();
                RecordMetadata rm= future==null? null: future.get();
                if(rm != null) {
                    System.out.printf("offset=%d, partition=%d\n", rm.offset(), rm.partition());
                }
                //next
                long delta= new Date().getTime() - now.getTime();
                if(delta > 1000)
                    continue;
                int sleep= (int)(1000 - delta);
                System.out.printf("Sleeping ... (%d ms)\n", sleep);
                Thread.sleep(sleep);
                System.out.println();
                count++;
            }// while
        } catch (Exception x) {
            x.printStackTrace();
        } finally {
            System.out.println("DONE!");

            // 4. 关闭
            simpleProducer.close();
        }// try/catch/finally
    }// main
}// class
