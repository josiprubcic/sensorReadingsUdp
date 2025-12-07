package drugiProjekt.kafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class SensorNode {
    private static final String TOPIC = "Command";

    public static void main(String[] args) {
        String sensorId = args.length > 0 ? args[0] : "1";

        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "sensor-" + sensorId); // različit group za svaki senzor [file:1]
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer =
                     new KafkaConsumer<>(consumerProperties)) {

            consumer.subscribe(Collections.singleton(TOPIC));
            System.out.println("Sensor " + sensorId + " čeka poruke na topicu " + TOPIC);

            boolean running = true;
            while (running) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(1000));

                records.forEach(record -> {
                    String value = record.value();
                    System.out.printf("Sensor %s primio: %s (partition=%d, offset=%d)%n",
                            sensorId, value, record.partition(), record.offset());

                    if ("Start".equals(value)) {
                        System.out.println("Sensor " + sensorId + ": START primljen");
                    } else if ("Stop".equals(value)) {
                        System.out.println("Sensor " + sensorId + ": STOP primljen");
                        // ovdje kasnije zatvoriš UDP itd., a za sada možeš prekinuti petlju
                        // running = false;  // trebaš ovo napraviti izvan lambda-e
                    }
                });
            }
        }
    }
}
