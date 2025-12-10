package drugiProjekt;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;
import java.util.Properties;
import java.util.Scanner;

public class Coordinator {
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String COMMAND_TOPIC = "Command";
    private static final String REGISTER_TOPIC = "Register";

    private KafkaProducer<String, String> producer;

    public Coordinator() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        this.producer = new KafkaProducer<>(props);
    }


    public void sendStartCommand() {
        JSONObject command = new JSONObject();
        command.put("type", "Start");
        command.put("timestamp", System.currentTimeMillis());

        ProducerRecord<String, String> record =
                new ProducerRecord<>(COMMAND_TOPIC, "start", command.toString());

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                System.err.println("Greška pri slanju Start: " + exception.getMessage());
            } else {
                System.out.println("Start poruka poslana na partition " + metadata.partition());
            }
        });
    }

    public void sendRegisterMessage(String nodeId) {
        JSONObject register = new JSONObject();
        register.put("type", "Register");
        register.put("nodeId", nodeId);
        register.put("timestamp", System.currentTimeMillis());

        ProducerRecord<String, String> record =
                new ProducerRecord<>(REGISTER_TOPIC, register.toString());

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                System.err.println("Greška pri slanju Register: " + exception.getMessage());
            } else {
                System.out.println("Register poruka poslana na partition " + metadata.partition());
            }
        });

    }

    public void sendStopCommand() {
        JSONObject command = new JSONObject();
        command.put("type", "Stop");
        command.put("timestamp", System.currentTimeMillis());

        ProducerRecord<String, String> record =
                new ProducerRecord<>(COMMAND_TOPIC, "stop", command.toString());

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                System.err.println("Greška pri slanju Stop: " + exception.getMessage());
            } else {
                System.out.println("Stop poruka poslana");
            }
        });
    }

    public void close() {
        producer.close(java.time.Duration.ofSeconds(5));  // ✅ timeout
    }

    public static void main(String[] args) throws InterruptedException {
        Coordinator coordinator = new Coordinator();
        System.out.println("Šaljem Start...");
        coordinator.sendStartCommand();

        // Čekaj 30 sekundi da senzori prorade
        Thread.sleep(30_000);

        System.out.println("Šaljem Stop i gasim se...");
        coordinator.sendStopCommand();

        // ✅ DODAJ OVO: čekaj da Stop poruka stvarno stigne
        Thread.sleep(2000);

        coordinator.close();
    }
}