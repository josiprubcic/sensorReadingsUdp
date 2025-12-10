package drugiProjekt.network;

import org.json.JSONObject;
import java.util.HashMap;
import java.util.Map;

/**
 * Vektorski sat za Lamportovu vremensku sinkronizaciju.
 * Svaki senzor održava mapu ID -> brojač za sve senzore u mreži.
 */
public class VectorClock {

    private final Map<String, Integer> clock;
    private final String ownSensorId;

    /**
     * Kreira novi vektorski sat za zadani senzor.
     * @param ownSensorId ID ovog senzora
     */
    public VectorClock(String ownSensorId) {
        this.ownSensorId = ownSensorId;
        this.clock = new HashMap<>();
        // Inicijaliziraj vlastiti sat na 0
        this.clock.put(ownSensorId, 0);
    }

    /**
     * Inkrementira brojač za ovaj senzor (poziva se prije slanja poruke).
     */
    public synchronized void increment() {
        //ako ključ SensorId ne postoji vrati 0
        int currentValue = clock.getOrDefault(ownSensorId, 0);
        //inkrementiraj
        clock.put(ownSensorId, currentValue + 1);
    }

    /**
     * Ažurira vektorski sat na temelju primljene poruke.
     * Primjenjuje pravilo: clock[i] = max(local[i], received[i]) za sve i.
     *
     * @param receivedClockJson JSON objekt primljenog vektorskog sata
     */
    public synchronized void update(JSONObject receivedClockJson) {
        // Za svaki element u primljenom satu
        for (String sensorId : receivedClockJson.keySet()) {
            int receivedValue = receivedClockJson.getInt(sensorId);
            int localValue = clock.getOrDefault(sensorId, 0);

            // Uzmi maksimum
            clock.put(sensorId, Math.max(localValue, receivedValue));
        }

        // Nakon update-a, inkrementiraj vlastiti sat
        increment();

    }

    /**
     * Vraća JSON reprezentaciju vektorskog sata.
     * @return JSONObject sa mapom sensorId -> brojač
     */
    public synchronized JSONObject toJson() {
        return new JSONObject(clock);
    }

    /**
     * Uspoređuje dva vektorska sata.
     * @return -1 ako je this < other, 1 ako je this > other, 0 ako su concurrent
     */
    public synchronized int compareTo(VectorClock other) {
        boolean thisSmaller = false;
        boolean thisGreater = false;

        // Kombiniraj sve ključeve iz oba sata
        Map<String, Integer> allKeys = new HashMap<>(this.clock);
        other.clock.forEach((k, v) -> allKeys.putIfAbsent(k, 0));

        for (String key : allKeys.keySet()) {
            int thisValue = this.clock.getOrDefault(key, 0);
            int otherValue = other.clock.getOrDefault(key, 0);

            if (thisValue < otherValue) {
                thisSmaller = true;
            } else if (thisValue > otherValue) {
                thisGreater = true;
            }
        }

        if (thisSmaller && !thisGreater) {
            return -1; // this happened before other
        } else if (thisGreater && !thisSmaller) {
            return 1; // other happened before this
        } else {
            return 0; // concurrent events
        }
    }

    /**
     * Kreira kopiju trenutnog vektorskog sata.
     */
    public synchronized VectorClock copy() {
        VectorClock copy = new VectorClock(this.ownSensorId);
        copy.clock.putAll(this.clock);
        return copy;
    }

    @Override
    public synchronized String toString() {
        return clock.toString();
    }

    /**
     * Getter za internu mapu (za debug/testiranje).
     */
    public synchronized Map<String, Integer> getClock() {
        return new HashMap<>(clock);
    }
}

