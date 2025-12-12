package drugiProjekt.model;

import org.json.JSONObject;
import java.util.UUID;

/**
 * Predstavlja podatkovni paket koji se šalje između senzora
 */
public class DataPacket {
    private final String messageId;
    private final String sensorId;
    private final String type;  // "DATA" ili "ACK"
    private final Double no2;
    private Long scalarTime;
    private final JSONObject vectorClock;

    //Konstruktor za DATA paket
    public DataPacket(String sensorId, Double no2, Long scalarTime, JSONObject vectorClock) {
        //kreira UUID za message id
        this.messageId = UUID.randomUUID().toString();
        this.type = "DATA";
        this.sensorId = sensorId;
        this.no2 = no2;
        this.scalarTime = scalarTime;
        this.vectorClock = vectorClock;
    }

    //Konstruktor za ACK paket
    //Postavlja praktički sve na null jer je ACKu nebitan sadržaj
    public DataPacket(String messageId, String sensorId) {
        this.messageId = messageId;
        this.type = "ACK";
        this.sensorId = sensorId;
        this.no2 = null;
        this.scalarTime = null;
        this.vectorClock = null;
    }

    // Constructor iz JSON-a (za parsiranje primljenih poruka)
    public DataPacket(JSONObject json) {
        this.messageId = json.getString("messageId");
        this.type = json.getString("type");

        if ("DATA".equals(type)) {
            this.sensorId = json.getString("sensorId");
            this.no2 = json.getDouble("no2");
            this.scalarTime = json.getLong("scalarTime");
            this.vectorClock = json.getJSONObject("vectorClock");
        } else {
            this.sensorId = json.optString("sensorId", null);
            this.no2 = null;
            this.scalarTime = null;
            this.vectorClock = null;
        }
    }

    public JSONObject toJson() {
        JSONObject json = new JSONObject();
        json.put("type", type);
        json.put("messageId", messageId);
        json.put("sensorId", sensorId);

        if ("DATA".equals(type)) {
            json.put("sensorId", sensorId);
            json.put("no2", no2);
            json.put("scalarTime", scalarTime);
            json.put("vectorClock", vectorClock);
        }

        return json;
    }

    // Getters
    public String getMessageId() { return messageId; }
    public String getType() { return type; }
    public String getSensorId() { return sensorId; }
    public Double getNo2() { return no2; }
    public Long getScalarTime() { return scalarTime; }
    public JSONObject getVectorClock() { return vectorClock; }
    public void setScalarTime(Long scalarTime) { this.scalarTime = scalarTime; }

    public boolean isData() { return "DATA".equals(type); }
    public boolean isAck() { return "ACK".equals(type); }
}
