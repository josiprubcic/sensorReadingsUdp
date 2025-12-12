package drugiProjekt.service;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InputStream;
import java.util.List;

public class SensorReadingService {

    private final List<CSVRecord> records;
    private long startTime;
    private static final int NO2_COLUMN_INDEX = 4; // NO2 je 5. stupac (Temperature,Pressure,Humidity,CO,NO2,SO2)

    //konstruktor koji prima ime CSV Filea (readings.csv)
    public SensorReadingService(String csvFileName) throws IOException {
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(csvFileName);

        if (inputStream == null) {
            throw new IOException("CSV datoteka nije pronađena u resources: " + csvFileName);
        }

        //Preskoči header i ignoriraj prazne redove
        CSVParser parser = CSVFormat.DEFAULT
                .builder()
                .setSkipHeaderRecord(true)
                .setIgnoreEmptyLines(true)
                .setTrim(true)
                .build()
                .parse(new InputStreamReader(inputStream));

        this.records = parser.getRecords();
        this.startTime = System.currentTimeMillis();
        System.out.println("Učitano " + records.size() + " redaka iz CSV-a");
    }

    public Double getNo2Reading() {
        //trenutno vrijeme zahtjeva za očitanjem
        long currentTime = System.currentTimeMillis();
        int brojAktivnihSekundi = (int) ((currentTime - startTime) / 1000);
        int rowIndex = (brojAktivnihSekundi % 100) ;

        if (rowIndex < records.size()) {
            CSVRecord record = records.get(rowIndex);

            //Koristi indeks, ne ime stupca
            //izbjegavanje Out of bounds greske
            if (record.size() > NO2_COLUMN_INDEX) {
                String no2Value = record.get(NO2_COLUMN_INDEX).trim();

                //Provjera je li vrijednost prazna
                if (!no2Value.isEmpty()) {
                    try {
                        return Double.parseDouble(no2Value);
                    } catch (NumberFormatException e) {
                        System.out.println("Nevaljan NO2 format: " + no2Value);
                        return null;
                    }
                }
            }
        }
        return null; // Nema očitanja za ovaj trenutak
    }
}
