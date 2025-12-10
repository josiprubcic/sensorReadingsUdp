/*
 * This code has been developed at Departement of Telecommunications,
 * Faculty of Electrical Engineering and Computing, University of Zagreb.
 */
package drugiProjekt.server;

import drugiProjekt.kafkaConsumer.SensorNode;
import drugiProjekt.network.SimpleSimulatedDatagramSocket;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.SocketException;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ClosedChannelException;

/**
 * @author Krešimir Pripužić
 */
public class StupidUDPServer {

    private final int port;
    private final SimpleSimulatedDatagramSocket socket;
    private final SensorNode sensorNode;

    public StupidUDPServer(int port, SensorNode sensorNode) throws SocketException {
        this.port = port;
        this.sensorNode = sensorNode;
        // 30% loss, 1000 ms delay – prema zadatku
        this.socket = new SimpleSimulatedDatagramSocket(port, 0.3, 1000);
    }

    public void start() {
        System.out.println("UDP server start na portu " + port);
        byte[] rcvBuf = new byte[256];

        while (true) {
            try {
                DatagramPacket packet = new DatagramPacket(rcvBuf, rcvBuf.length);
                socket.receive(packet);   // blokira dok ne dođe paket ili se socket ne zatvori

                String msg = new String(packet.getData(),
                        packet.getOffset(),
                        packet.getLength());
                System.out.println("Server received on " + port + ": " + msg);

                sensorNode.processReceivedPacket(msg);

            } catch (AsynchronousCloseException e) {
                System.out.println("UDP server na portu " + port + " se gasi (AsynchronousCloseException)");
                break;

            } catch (ClosedChannelException e) {
                System.out.println("UDP server na portu " + port + " se gasi (ClosedChannelException)");
                break;

            } catch (IOException e) {
                System.out.println("UDP server na portu " + port + " I/O greška: "
                        + e.getClass().getSimpleName() + " - " + e.getMessage());
                break;
            }
        }
    }






    public void close(){
        socket.close();
    }


    public static void main(String[] args) throws Exception {
    }
}
