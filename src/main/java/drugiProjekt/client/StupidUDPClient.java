package drugiProjekt.client;

import drugiProjekt.network.SimpleSimulatedDatagramSocket;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.SocketException;

public class StupidUDPClient {

    private final SimpleSimulatedDatagramSocket socket;

    public StupidUDPClient() throws SocketException {
        // loss 30%, delay 1000 ms
        this.socket = new SimpleSimulatedDatagramSocket(0.3, 1000);
    }

    public void send(String message, String address, int port) throws IOException {
        byte[] sendBuf = message.getBytes();
        InetAddress inetAddress = InetAddress.getByName(address);

        DatagramPacket packet =
                new DatagramPacket(sendBuf, sendBuf.length, inetAddress, port);

        socket.send(packet);  // SENDTO
        System.out.println("UDP klijent poslao: \"" + message +
                "\" na " + address + ":" + port);
    }

    public void close() {
        socket.close();
    }
}
