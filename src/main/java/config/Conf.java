package config;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;

public class Conf {

//    public static String DATA_PATH = "/data/clean-data-csv/";
    public static String DATA_PATH = "/home/zhili/Documents/UIUC_MCS/Cloud_Computing_Capstone/Task_2/test/";

    /* Kafka Configurations */
    public static String KAFKA_SERVERS = "localhost:9092";
//    public static String KAFKA_SERVERS =
//            "b-2.kafkacluster2.1n3omu.c3.kafka.us-east-1.amazonaws.com:9092," +
//            "b-3.kafkacluster2.1n3omu.c3.kafka.us-east-1.amazonaws.com:9092," +
//            "b-1.kafkacluster2.1n3omu.c3.kafka.us-east-1.amazonaws.com:9092";

    public static Map<String, Object> KAFKA_PARAMS = new HashMap<String, Object>() {{
        put("bootstrap.servers", KAFKA_SERVERS);
//        put("security.protocol", "SSL");
//        put("ssl.truststore.location", "/tmp/kafka.client.truststore.jks");
        put("key.serializer", StringSerializer.class);
        put("key.deserializer", StringDeserializer.class);
        put("value.serializer", StringSerializer.class);
        put("value.deserializer", StringDeserializer.class);
        put("group.id", "ccc");
        put("auto.offset.reset", "earliest");
        put("enable.auto.commit", false);
    }};


    public static String TOPIC = "flights";


    /* Spark Streaming Configurations */
    public static String CHECKPOINT_DIR = "/home/hadoop/checkpoint";

    /* Cassandra Configurations */
    public static String CASSANDRA_HOST = "54.144.198.101";
    public static String CASSANDRA_PORT = "9042";

    public static String KEYSPACE_BATCH = "batch";
    public static String KEYSPACE_STREAMING = "streaming";
    public static String TABLE11 = "results11";
    public static String TABLE12 = "results12";
    public static String TABLE21 = "results21";
    public static String TABLE22 = "results22";
    public static String TABLE24 = "results24";
    public static String TABLE32 = "results32";

}
