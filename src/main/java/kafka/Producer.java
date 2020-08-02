package kafka;

import config.Conf;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;


public class Producer implements Runnable {

    private File file;

    public Producer(File file) {
        this.file = file;
    }

    @Override
    public void run() {
        if (file.getName().endsWith(".csv")) {
            try (BufferedReader reader = Files.newBufferedReader(file.toPath())) {
                KafkaProducer<String, String> producer = new KafkaProducer<String, String>(Conf.KAFKA_PARAMS);
                Iterable<CSVRecord> records = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(reader);

                for (CSVRecord record : records) {
                    String year = record.get("Year");
                    String dayOfWeek = record.get("DayOfWeek");
                    String flightDate = record.get("FlightDate");
                    String uniqueCarrier = record.get("UniqueCarrier");
                    String flightNum = record.get("FlightNum");
                    String origin = record.get("Origin");
                    String dest = record.get("Dest");
                    String cRSDepTime = record.get("CRSDepTime");
                    String depDelay = record.get("DepDelay");
                    String arrDelay = record.get("ArrDelay");

                    String message = String.join(",", new String[]{year, dayOfWeek, flightDate,
                            uniqueCarrier, flightNum, origin, dest, cRSDepTime, depDelay, arrDelay});
                    producer.send(new ProducerRecord<String, String>(Conf.TOPIC, message));
                }
                producer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

