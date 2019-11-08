import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaProbe {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaProbe.class.getName());
    private static final String TOPIC = "tttopic";
    private static final String HOST = "localhost:9092";
    private static boolean flagExit = false;

    public static void main(String[] args) {
        startTopic();

        new Thread(
                () -> {runProducerWithUserInput();
                flagExit = true;
                }
        ).start();

        new Thread(
                () -> runConsumer()
        ).start();
    }

    private static void runConsumer(){
        //creating properties for consumer
        Properties props = new Properties();
        //bootstrapping list of brokers
        props.put("bootstrap.servers", HOST);
        props.put("group.id", "myCons");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        //creating consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(TOPIC));
        //geting records from the topic
        while (!flagExit) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, value = %s%n",
                        record.offset(), record.value());
        }
        consumer.close();
    }

    private static void runProducerWithUserInput(){
        //creating properties for producer
        Properties props = new Properties();
        props.put("bootstrap.servers", HOST);
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //creating producer using properties
        Producer<String, String> producer = new KafkaProducer<>(props);

        //sending from console
        System.out.println("Write smth or enter exit");

        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String line;
        try {
            while (!(line = reader.readLine()).equals("exit")){
                producer.send(new ProducerRecord<String, String>(TOPIC, line));
            }
        }
        catch(IOException e){
            LOG.error("runProducerWithUserInput caught {}", e.getClass().getName());
            LOG.error("Stack trace {}", e.getStackTrace());
        }
        finally {
            //closing
            producer.close();
        }
    }

    private static void startTopic(){
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, HOST);

        AdminClient admin = AdminClient.create(props);

        Map<String, String> configs = new HashMap<>();
        int partitions = 1;
        short replication = 1;

        CreateTopicsResult result =
                admin.createTopics(Arrays.asList(new NewTopic(TOPIC, partitions, replication).configs(configs)));
        result.all();
    }
}
