package ru.bigdataflink;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CsvToKafkaProducer {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        String bootstrapServers = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092");
        String topic = env("KAFKA_TOPIC", "sales-json");
        Path dataDir = Paths.get(env("DATA_DIR", "исходные данные"));

        createTopicIfNeeded(bootstrapServers, topic);

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all");

        long sent = 0;
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            List<Path> csvFiles;
            try (Stream<Path> files = Files.list(dataDir)) {
                csvFiles = files
                        .filter(path -> path.getFileName().toString().toLowerCase().endsWith(".csv"))
                        .sorted()
                        .collect(Collectors.toList());
            }

            for (Path csvFile : csvFiles) {
                sent += sendFile(producer, topic, csvFile);
            }
            producer.flush();
        }

        System.out.println("Sent " + sent + " records to Kafka topic " + topic);
    }

    private static long sendFile(KafkaProducer<String, String> producer, String topic, Path csvFile) throws Exception {
        long sent = 0;
        try (Reader reader = Files.newBufferedReader(csvFile, StandardCharsets.UTF_8);
             CSVParser parser = CSVFormat.DEFAULT.builder()
                     .setHeader()
                     .setSkipHeaderRecord(true)
                     .build()
                     .parse(reader)) {
            List<String> headers = parser.getHeaderNames();
            for (CSVRecord record : parser) {
                Map<String, String> json = new LinkedHashMap<>();
                for (String header : headers) {
                    String value = record.get(header);
                    json.put(header, value == null || value.isBlank() ? null : value);
                }

                String key = json.get("id");
                producer.send(new ProducerRecord<>(topic, key, OBJECT_MAPPER.writeValueAsString(json)));
                sent++;
            }
        }
        return sent;
    }

    private static void createTopicIfNeeded(String bootstrapServers, String topic) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);

        try (AdminClient adminClient = AdminClient.create(props)) {
            if (!adminClient.listTopics().names().get().contains(topic)) {
                adminClient.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1))).all().get();
            }
        }
    }

    private static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
