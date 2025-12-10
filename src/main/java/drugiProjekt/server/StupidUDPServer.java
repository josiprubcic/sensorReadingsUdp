/*
 * This code has been developed at Departement of Telecommunications,
 * Faculty of Electrical Engineering and Computing, University of Zagreb.
 */
package drugiProjekt.server;

import drugiProjekt.network.SimpleSimulatedDatagramSocket;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.SocketException;
import java.nio.channels.AsynchronousCloseException;

/**
 * @author Krešimir Pripužić
 */
public class StupidUDPServer {

    private final int port;
    private final SimpleSimulatedDatagramSocket socket;

    public StupidUDPServer(int port) throws SocketException {
        this.port = port;
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
                // ovdje kasnije parsirati očitanja

            } catch (AsynchronousCloseException e) {
                System.out.println("UDP server na portu " + port + " se gasi (AsynchronousCloseException)");
                break;

            } catch (java.nio.channels.ClosedChannelException e) {
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
        StupidUDPServer server = new StupidUDPServer(10001);
        server.start();
    }
}
