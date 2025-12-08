package drugiProjekt.model;

public class Peer {
    private final String id;
    private final String address;
    private final int port;

    public Peer(String id, String address, int port) {
        this.id = id;
        this.address = address;
        this.port = port;
    }

    public String getId() { return id; }
    public String getAddress() { return address; }
    public int getPort() { return port; }

    @Override
    public String toString() {
        return "Peer{id=" + id + ", address=" + address + ", port=" + port + "}";
    }
}
