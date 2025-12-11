package drugiProjekt.network;

import drugiProjekt.client.StupidUDPClient;
import drugiProjekt.model.DataPacket;
import drugiProjekt.model.Peer;
import org.json.JSONObject;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Obrađuje primljene DATA i ACK pakete
 */
public class MessageHandler {
    private final Set<String> receivedMessages = ConcurrentHashMap.newKeySet();
    private final StupidUDPClient udpClient;
    private final VectorClock vectorClock;
    private final String sensorId;
    private final List<Peer> peers;

    public MessageHandler(StupidUDPClient udpClient, VectorClock vectorClock,
                          String sensorId, List<Peer> peers) {
        this.udpClient = udpClient;
        this.vectorClock = vectorClock;
        this.sensorId = sensorId;
        this.peers = peers;
    }

    /**
     * Obrađuje DATA paket
     */
    public void handleDataPacket(DataPacket packet) {
        String messageId = packet.getMessageId();
        String senderId = packet.getSensorId();

        // Provjera duplikata
        if (receivedMessages.contains(messageId)) {
            //Ako je duplikat radi se samo log i ponovno se pošalje ACK da pošiljatelj makne tu poruku iz pending ACKs
            String shortId = messageId.substring(0, 8);
            System.out.printf("[DUPL] S%s ← #%s%n", sensorId, shortId);
            sendAck(messageId, senderId);
            return;
        }

        // Označi kao primljen
        receivedMessages.add(messageId);

        // Ažuriraj vektorski sat
        vectorClock.update(packet.getVectorClock());



        // TODO: Spremi podatak za sortiranje kasnije

        // Pošalji ACK
        sendAck(messageId, senderId);
    }

    /**
     * Šalje ACK potvrdu
     */
    private void sendAck(String messageId, String targetSensorId) {
        DataPacket ackPacket = new DataPacket(messageId);  // ACK constructor

        Peer targetPeer = findPeerById(targetSensorId);
        if (targetPeer != null) {
            try {
                udpClient.send(ackPacket.toJson().toString(),
                        targetPeer.getAddress(),
                        targetPeer.getPort());
            } catch (IOException e) {
                System.err.println("GREŠKA pri slanju ACK: " + e.getMessage());
            }

        }
    }

    /**
     * Traži peer prema ID-u
     */
    private Peer findPeerById(String peerId) {
        synchronized (peers) {
            for (Peer p : peers) {
                if (p.getId().equals(peerId)) {
                    return p;
                }
            }
        }
        return null;
    }
}
