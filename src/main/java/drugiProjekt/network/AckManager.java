package drugiProjekt.network;

import drugiProjekt.client.StupidUDPClient;
import drugiProjekt.model.DataPacket;
import drugiProjekt.model.Peer;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Upravlja ACK potvrdom i retransmisijom paketa
 */
public class AckManager {
    //Concurrent HashMap omogućuje sigurnost za više dretvi, sadrži sve trenutno poslane i nepotvrđene poruke
    private final Map<String, PendingMessage> pendingAcks = new ConcurrentHashMap<>();
    private final StupidUDPClient udpClient;
    private final String sensorId;

    //Vrijeme čekanja ACKa 2 sekunde
    private static final long ACK_TIMEOUT_MS = 2000;
    //maksimalan broj retransmisija
    private static final int MAX_RETRIES = 5;
    private volatile boolean running = true;

    //sadrži informacije o poruci koja čeka ACK
    private static class PendingMessage {
        DataPacket packet;
        Peer peer;
        //trenutak kada je zadnji put poslan paket
        long sentTime;
        //kolicina retransmisija
        int retryCount;

        PendingMessage(DataPacket packet, Peer peer) {
            this.packet = packet;
            this.peer = peer;
            this.sentTime = System.currentTimeMillis();
            this.retryCount = 0;
        }
    }

    //inicijalizira AckManager za jedan senzor i pali pozadinski thread koji se brine o retransmisiji
    public AckManager(StupidUDPClient udpClient, String sensorId) {
        this.udpClient = udpClient;
        this.sensorId = sensorId;
        startRetransmissionThread();
    }

    /**
     * Šalje podatkovni paket i čeka ACK
     * U pendingAcks stavlja novi key vezan za PendingMessage objekt
     */
    public void sendWithAck(DataPacket packet, Peer peer) {
        //key = id poruke + id senzora susjeda
        String key = packet.getMessageId() +  peer.getId();
        pendingAcks.put(key, new PendingMessage(packet, peer));

        try {
            udpClient.send(packet.toJson().toString(), peer.getAddress(), peer.getPort());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * Potvrđuje primitak paketa (uklanja iz pending liste)
     */
    public boolean confirmReceived(String messageId, String senderId) {
        //exactKey mora odgovarati keyu is PendingAcks
        String exactKey = messageId + senderId;
        //vrati True ako je postojao i uklonjen je exactKey
        return pendingAcks.remove(exactKey) != null;
    }

    /**
     * Thread za automatsku retransmisiju
     */
    private void startRetransmissionThread() {
        Thread retransmissionThread = new Thread(() -> {
            while (running) {
                try {
                    long now = System.currentTimeMillis();

                    for (Map.Entry<String, PendingMessage> entry : pendingAcks.entrySet()) {
                        PendingMessage pending = entry.getValue();

                        if (now - pending.sentTime > ACK_TIMEOUT_MS) {
                            if (pending.retryCount < MAX_RETRIES) {
                                // RETRANSMISIJA
                                pending.retryCount++;
                                pending.sentTime = now;

                                udpClient.send(pending.packet.toJson().toString(),
                                        pending.peer.getAddress(),
                                        pending.peer.getPort());

                                System.out.println("Sensor " + sensorId + " RETRANSMIT msgId=" +
                                        pending.packet.getMessageId() +
                                        " -> " + pending.peer.getId() +
                                        " pokušaj " + pending.retryCount);
                            } else {
                                // Premašen broj pokušaja
                                System.err.println("Sensor " + sensorId + " FAILED msgId=" +
                                        pending.packet.getMessageId() +
                                        " -> " + pending.peer.getId());
                                pendingAcks.remove(entry.getKey());
                            }
                        }
                    }

                    Thread.sleep(1000);
                } catch (Exception e) {
                    if (running) e.printStackTrace();
                }
            }
        });
        retransmissionThread.setName("AckManager-Retransmission-" + sensorId);
        retransmissionThread.start();
    }

    public void shutdown() {
        running = false;
    }
}
