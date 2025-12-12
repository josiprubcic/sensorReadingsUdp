package drugiProjekt.kafkaConsumer;

import drugiProjekt.client.StupidUDPClient;
import drugiProjekt.model.DataPacket;
import drugiProjekt.network.AckManager;
import drugiProjekt.network.EmulatedSystemClock;
import drugiProjekt.network.MessageHandler;
import drugiProjekt.network.VectorClock;
import drugiProjekt.server.StupidUDPServer;
import drugiProjekt.model.Peer;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


import drugiProjekt.service.SensorReadingService;
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
    private final List<DataPacket> readings = new ArrayList<>();
    private long lastSortTime = 0;
    private SensorReadingService readingService;
    private EmulatedSystemClock emulatedClock;
    private VectorClock vectorClock;
    private AckManager ackManager;
    private MessageHandler messageHandler;

    private volatile boolean shutdownRequested = false;
    private static final String[] TOPICS = {"Command", "Register"};
    private static final String REGISTER_TOPIC = "Register";

    private StupidUDPServer udpServer;
    private StupidUDPClient udpClient;
    private final List<Peer> peers = new ArrayList<>();
    private final String sensorId;
    private final int udpPort;
    private KafkaProducer<String, String> producer;
    private KafkaConsumer<String, String> consumer;

    /**konstruktor
     * kreira i inicijalizira sensor sa sensorId i udpPortom
     * pokreće emulatedClock senzora
     * pokreće VectorClosk senzora
     * pokreće readingService i učitava u njega readings.csv
     * pokrece udpServer senzora - preko njega klijent senzor prima pakete
     * pokrece udpClient senzora - preko njega klijent šalje očitanja NO2
     * pokreće ackManagera - brine se da podaci uistinu stignu, vrši retransmisiju
     * pokreće messageHandler
     * postavlja zadnje vrijeme sortiranja preko emulatedClocka
     */
    public SensorNode(String sensorId, int udpPort) throws IOException {
        this.sensorId = sensorId;
        this.udpPort = udpPort;
        this.emulatedClock = new EmulatedSystemClock();
        this.vectorClock = new VectorClock(sensorId);
        this.readingService = new SensorReadingService("readings.csv");
        this.udpServer = new StupidUDPServer(udpPort, this);
        this.udpClient = new StupidUDPClient();
        this.ackManager = new AckManager(udpClient, sensorId);
        this.messageHandler = new MessageHandler(udpClient, vectorClock, sensorId, peers, this.readings);
        this.lastSortTime = emulatedClock.currentTimeMillis();
        // postavlja konfiguraciju i kreira KafkaProducer instancu
        initProducer();
    }

    /**
     * Inicijalizira Kafka producera s potrebnim postavkama za slanje poruka.
     * Producer se koristi za objavu registracijskih poruka na "Register" topic.
     */
    private void initProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put("connections.max.idle.ms", "600000");
        props.put("retries", "3");
        producer = new KafkaProducer<>(props);
    }
    /**
     * Objavljuje registracijsku poruku (ID, adresa, port) na Kafka topic "Register".
     * Ostali senzori primaju ovu poruku i dodaju ovaj čvor u listu peers.
     */
    private void sendRegister(int port) {
        JSONObject json = new JSONObject();
        json.put("id", sensorId);
        json.put("address", "localhost");
        json.put("port", String.valueOf(port));

        //producer je inicijaliziran prethodno u konstruktoru metodom initProducer();
        producer.send(new ProducerRecord<>(REGISTER_TOPIC, sensorId, json.toString()));
        System.out.println("Sensor " + sensorId + " poslao Register poruku");
    }

    private KafkaConsumer<String, String> initConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "sensor-group-" + sensorId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");
        return new KafkaConsumer<>(props);
    }

    /**
     * Glavna petlja senzora:
     * 1. Inicijalizira Kafka consumera i pretplaćuje se na "Command" i "Register" topice
     * 2. Čeka "Start" poruku od koordinatora
     * 3. Nakon "Start", registrira se, pokreće UDP server i započinje slanje očitanja
     * 4. Na "Stop" poruku zaustavlja sve komponente
     */
    private void run() {
        consumer = initConsumer();
        consumer.subscribe(asList(TOPICS));

        System.out.println("Sensor " + sensorId + " sluša Command + Register");

        try {
            while (!shutdownRequested) {
                //svakih 100 ms provjeri kafka poruke
                var records = consumer.poll(Duration.ofMillis(100));
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


                                new Thread(() -> udpServer.start()).start();
                                //senzor se registrira i uz registraciju veze svoj UDP port kako bi mogao primati UDP pakete
                                sendRegister(udpPort);

                                new Thread(() -> {
                                    while (!shutdownRequested) {
                                        try {
                                            //lista snapshot za svaku dretvu
                                            List<Peer> snapshot;
                                            //peers smije pristupati samo jedna dretva dok se izvršava proces
                                            synchronized (peers) {
                                                snapshot = new ArrayList<>(peers);
                                            }
                                            // Dohvati NO2 očitanje
                                            Double no2 = readingService.getNo2Reading();

                                            if (no2 != null) {
                                                //inkrement vektorskog sata prije slanja
                                                vectorClock.increment();
                                                // Kreiranje DataPacket objekta tipa DATA
                                                DataPacket packet = new DataPacket(
                                                        sensorId,
                                                        no2,
                                                        emulatedClock.currentTimeMillis(),
                                                        vectorClock.toJson()
                                                );

                                                synchronized (readings) {
                                                    readings.add(packet);
                                                }
                                                // Pošalji svim peerovima prethodno kreiran paket, s ACKom
                                                for (Peer p : snapshot) {
                                                    ackManager.sendWithAck(packet, p);

                                                    String shortId = packet.getMessageId().substring(0, 8);
                                                    System.out.printf(
                                                            "[->] S%s -> S%s | NO2=%.0f | Vec:%s | ts=%d | #%s%n",
                                                            sensorId, p.getId(), no2, vectorClock,
                                                            packet.getScalarTime(),   // getter u DataPacket
                                                            shortId
                                                    );
                                                }


                                            } else {
                                                System.out.println("Sensor " + sensorId + " nema NO2 očitanje za ovaj trenutak");
                                            }
                                            long now = System.currentTimeMillis();
                                            if (now - lastSortTime >= 5000) {
                                                sortAndPrintReadings();
                                                lastSortTime = now;
                                            }
                                            Thread.sleep(1000);
                                        } catch (Exception e) {
                                            e.printStackTrace();
                                            break;
                                        }
                                    }
                                }).start();

                            } else if ("Stop".equals(type)) {
                                System.out.println("Sensor " + sensorId + ": STOP!");
                                stopEverything(); //stopira KAFKU, UDP...
                                return;
                            }
                        } else if ("Register".equals(topic)) {
                            String id = msg.getString("id");
                            String address = msg.getString("address");
                            String port = msg.getString("port");

                            // dodavanje samo susjeda, ne sebe
                            if (!id.equals(sensorId)) {
                                synchronized (peers) {
                                    peers.add(new Peer(id, address, Integer.parseInt(port)));
                                }
                                System.out.println("Sensor " + sensorId + " registrirao: id=" + id +
                                        ", address=" + address + ", port=" + port);
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("Nevaljan JSON: " + value);
                    }
                }
            }
        } finally {
            kafkaCleanup();
        }
    }

    /**
     * Procesira primljeni UDP paket i ažurira vektorski i skalarni sat.
     * Poziva se iz UDP servera.
     */
    public void processReceivedPacket(String message) {
        try {
            //System.out.println("RAW UDP: " + message);
            JSONObject json = new JSONObject(message);
            DataPacket packet = new DataPacket(json);  //Parsiranje u DataPacket

            if (packet.isData()) {
                long recvTs  = packet.getScalarTime();
                long localTs = emulatedClock.currentTimeMillis();

                //Sinkronizacija emuliranog skalarnog sata senzora
                long adjusted = emulatedClock.syncWith(packet.getScalarTime());





                String shortId = packet.getMessageId().substring(0, 8);
                System.out.printf(
                        "[<-] S%s <- S%s | NO2=%.0f | ts=%d (local=%d -> adj=%d) | #%s%n",
                        sensorId, packet.getSensorId(), packet.getNo2(),
                        recvTs, localTs, adjusted, shortId
                );



                messageHandler.handleDataPacket(packet);  //Delegiraj na handler
                //Handler šalje ACK
            } else if (packet.isAck()) {
                //potvrdi ack
                //System.out.println("*** ACK STIGAO: " + packet.getMessageId());
               // System.out.println("Ključ: " + packet.getMessageId() + packet.getSensorId());
                boolean removed = ackManager.confirmReceived(packet.getMessageId(), packet.getSensorId());
                if (removed) {
                    String shortId = packet.getMessageId().substring(0, 8);
                    System.out.printf("[USPJEŠNO] S%s ACK #%s%n", sensorId, shortId);
                }


            }

        } catch (Exception e) {
            System.out.println("Greška pri procesiranju paketa: " + e.getMessage());
        }
    }

    private void sortAndPrintReadings() {

     //   debugReadings("before window");
        long now = emulatedClock.currentTimeMillis();

        List<DataPacket> window;
        synchronized (readings) {
            window = readings.stream()
                    .filter(p -> p.getScalarTime() != null && now - p.getScalarTime() <= 5000)
                    .toList();
        }
       // System.out.println("DEBUG " + sensorId + " window size=" + window.size());

        if (window.isEmpty()) {
            System.out.println("Sensor " + sensorId + " nema očitanja u zadnjih 5s.");
            return;
        }

        //Sort po SKALARNOM vremenu
        List<DataPacket> byScalar = new ArrayList<>(window);
        byScalar.sort((a, b) -> Long.compare(a.getScalarTime(), b.getScalarTime()));

        System.out.println("=== S" + sensorId + " SORT SKALAR ===");
        for (DataPacket p : byScalar) {
            System.out.printf("S%s NO2=%.0f ts=%d id=%s%n",
                    p.getSensorId(), p.getNo2(), p.getScalarTime(),
                    p.getMessageId().substring(0, 8));
        }

        //Sort po VEKTORSKOM vremenu – koristi VectorClock.compareTo
        List<DataPacket> byVector = new ArrayList<>(window);
        byVector.sort((a, b) -> {
            VectorClock va = new VectorClock(sensorId);
            va.update(a.getVectorClock());
            VectorClock vb = new VectorClock(sensorId);
            vb.update(b.getVectorClock());
            return va.compareTo(vb);
        });

        System.out.println("=== S" + sensorId + " SORT VEKTOR ===");
        for (DataPacket p : byVector) {
            System.out.printf("S%s NO2=%.0f Vec=%s id=%s%n",
                    p.getSensorId(), p.getNo2(), p.getVectorClock(),
                    p.getMessageId().substring(0, 8));
        }

        double avg = window.stream()
                .mapToDouble(DataPacket::getNo2)
                .average()
                .orElse(0.0);
        System.out.printf("S%s PROSJEČNO NO2 u zadnjih 5s: %.2f (n=%d)%n",
                sensorId, avg, window.size());
    }


    private void stopEverything() {
        System.out.println("Sensor " + sensorId + " pokreće shutdown...");
        shutdownRequested = true;

        if (ackManager != null) {
            ackManager.shutdown();
        }

        //Čekanje da svi UDP paketi u redu završe (1000ms delay + buffer)
        try {
            System.out.println("Čekam da UDP paketi završe...");
            Thread.sleep(1500);
        } catch (InterruptedException ignored) {}

        // Sada zatvori - nema pending paketa
        if (udpClient != null) udpClient.close();
        if (udpServer != null) udpServer.close();

        System.out.println("UDP zatvoren");
    }


    private void kafkaCleanup() {
        if (producer != null) {
            try {
                producer.close(Duration.ofSeconds(5));
                System.out.println("Sensor " + sensorId + " producer ugašen");
            } catch (Exception e) {
                System.out.println("Producer greška: " + e.getMessage());
            }
        }
        if (consumer != null) {
            try {
                consumer.close(Duration.ofSeconds(5));
                System.out.println("Sensor " + sensorId + " consumer ugašen");
            } catch (Exception e) {
                System.out.println("Consumer greška: " + e.getMessage());
            }
        }
    }

    private void debugReadings(String where) {
        synchronized (readings) {
            System.out.println("DEBUG " + sensorId + " [" + where + "] readings.size=" + readings.size());
            readings.stream()
                    .limit(5)
                    .forEach(p -> System.out.printf(
                            "  NO2=%.0f ts=%d from=%s id=%s%n",
                            p.getNo2(), p.getScalarTime(), p.getSensorId(),
                            p.getMessageId().substring(0, 8)
                    ));
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.err.println("Usage: java SensorNode <id> <port>");
            System.exit(1);
        }
        String sensorId = args[0];
        int udpPort = Integer.parseInt(args[1]);
        SensorNode node = new SensorNode(sensorId, udpPort);
        node.run();
    }
}
