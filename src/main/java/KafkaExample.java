import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Date;
import java.util.Optional;
import java.util.Properties;

public class KafkaExample {
    private final String brokers;
    private final String topic;

    public KafkaExample(String brokers, String topic) {
        this.brokers = brokers;
        this.topic = topic;
    }

    public void consume() {
        Logger log = LoggerFactory.getLogger("kafaka-consumer");
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "demo-consumer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configureTls(props);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                log.info("{} [{}] offset={}, key={}, value=\"{}\"",
                        record.topic(), record.partition(),
                        record.offset(), record.key(), record.value());
            }
        }
    }

    public void produce() {
        Logger log = LoggerFactory.getLogger("kafaka-producer");
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configureTls(props);
        Thread one = new Thread() {
            public void run() {
                try {
                    Producer<String, String> producer = new KafkaProducer<>(props);
                    int i = 0;
                    while (true) {
                        Date d = new Date();
                        producer.send(new ProducerRecord<>(topic, Integer.toString(i), d.toString()));
                        Thread.sleep(1000);
                        i++;
                    }
                } catch (InterruptedException v) {
                    log.info("interrupted", v);
                }
            }
        };
        one.start();
    }

    public void configureTls(Properties props) {
        props.put("security.protocol", "SSL");
        props.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, "TLSv1.2");
        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "/tmp/kafka-keystore.jks");
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "changeme");
        props.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PKCS12");
        //props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "changeme");
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/tmp/kafka-truststore.jks");
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "changeme");
        props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PKCS12");
    }

    public static void main(String[] args) {
        String topic = Optional.ofNullable(System.getenv("KAFKA_TOPIC")).orElse("hello");
        String brokers = Optional.ofNullable(System.getenv("KAFKA_BROKERS")).orElse("localhost:9092");
        KafkaExample c = new KafkaExample(brokers, topic);
        c.produce();
        c.consume();
    }
}
