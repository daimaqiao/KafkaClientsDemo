package dmq.test.kafka.mongodb;

import dmq.test.kafka.SimpleProducer;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

// 使用SimpleProducer向kafka推送Fish.json.txt
// 这个不是mongodb的官方代码 :-D
public class PushFishToKafka {
    private static final String FISH_FILE= "Fish.json.txt";
    private static final String KAFKA_SERVERS= "192.168.0.1:9092";
    private static final String KAFKA_TOPIC= "clusterdb-topic1";

    public static void main(String[] args) {
        SimpleProducer producer= new SimpleProducer(KAFKA_SERVERS, KAFKA_TOPIC);
        Properties properties= new Properties();
        properties.put("bootstrap.servers", KAFKA_SERVERS);
        properties.put("client.id", PushFishToKafka.class.getSimpleName());
        properties.put("batch.size", 4096);
        properties.put("linger.ms", 0);
        properties.put("max.block.ms", 1000);
        properties.put("metadata.fetch.timeout.ms", 5000);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer.open(properties);

        InputStream stream= PushFishToKafka.class.getClassLoader().getResourceAsStream(FISH_FILE);
        try {
            BufferedReader reader= new BufferedReader(new InputStreamReader(stream));
            String line= reader.readLine();
            int n= 0;
            while(line != null) {
                n++;
                System.out.printf("%d -- %s\n", n, line);
                producer.send("fish", line);
                line= reader.readLine();
            }// while
        }
        catch(Exception x) {
            x.printStackTrace();
        }
        finally {
            if (stream != null) {
                try {
                    stream.close();
                } catch (Exception x2) {
                    x2.printStackTrace();
                }
            }
            producer.close();
        }// try
    }// main
}
