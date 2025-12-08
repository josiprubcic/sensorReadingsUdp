package drugiProjekt.kafkaConsumer;

import drugiProjekt.model.Peer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;

import static java.util.Arrays.asList;

public class SensorNode {
    private static final String[] TOPICS = {"Command", "Register"};
    private static final String REGISTER_TOPIC = "Register";

    // više NIJE static → svaka instanca senzora ima svoju listu
    private final List<Peer> peers = new ArrayList<>();

    private final String sensorId;

    public SensorNode(String sensorId) {
        this.sensorId = sensorId;
    }

    private void sendRegister(int port) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String,String> producer = new KafkaProducer<>(props);

        JSONObject json = new JSONObject();
        json.put("id", sensorId);
        json.put("address", "localhost");
        json.put("port", String.valueOf(port));

        producer.send(new ProducerRecord<>(REGISTER_TOPIC, sensorId, json.toString()));
        producer.close();
    }

    private void run() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "sensor-" + sensorId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(asList(TOPICS));

        System.out.println("Sensor " + sensorId + " sluša Command + Register");

        try {
            while (true) {
                var records = consumer.poll(Duration.ofMillis(1000));
                for (var record : records) {
                    String topic = record.topic();
                    String value = record.value();

                    System.out.println("Sensor " + sensorId + " [" + topic + "]: " + value);

                    try {
                        JSONObject msg = new JSONObject(value);

                        if ("Command".equals(topic)) {
                            String type = msg.getString("type");
                            if ("Start".equals(type)) {
                                System.out.println("Sensor " + sensorId + ": START!");
                                sendRegister(8000 + Integer.parseInt(sensorId));
                            } else if ("Stop".equals(type)) {
                                System.out.println("Sensor " + sensorId + ": STOP!");
                                return;
                            }
                        } else if ("Register".equals(topic)) {
                            String id = msg.getString("id");
                            String address = msg.getString("address");
                            String port = msg.getString("port");

                            peers.add(new Peer(id, address, Integer.parseInt(port)));

                            System.out.println("Sensor " + sensorId +
                                    " registrirao: id=" + id +
                                    ", address=" + address +
                                    ", port=" + port);
                        }
                    } catch (Exception e) {
                        System.out.println("Nevaljan JSON: " + value);
                    }
                }
            }
        } finally {
            consumer.close();
        }
    }

    public static void main(String[] args) {
        String sensorId = args.length > 0 ? args[0] : "1";
        SensorNode node = new SensorNode(sensorId);
        node.run();
    }
}
