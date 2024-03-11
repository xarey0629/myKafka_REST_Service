package com.example.restservice;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;

public class MyProducer {
    Properties kafkaPros;
    MyProducer() throws IOException {
        this.kafkaPros = MyProducer.loadConfig("configfile");
    }

    // We'll reuse this function to load properties from the Consumer as well
    public static Properties loadConfig(final String configFile) throws IOException {
        if (!Files.exists(Paths.get(configFile))) {
            throw new IOException(configFile + " not found.");
        }
        final Properties cfg = new Properties();
        try (InputStream inputStream = new FileInputStream(configFile)) {
            cfg.load(inputStream);
        }
        return cfg;
    }

    public String consume(String topicName, JsonNode[] configArray) throws IOException, InterruptedException{
        for (JsonNode config : configArray) {
            String key = config.fieldNames().next();
            String value = config.get(key).asText();
            kafkaPros.setProperty(key, value);
        }
        var consumer = new KafkaConsumer(kafkaPros);
        consumer.subscribe(Collections.singletonList(topicName));
        while(true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records){
                System.out.println("Received a message:");
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                return record.value();
            }
//            else{
//                System.out.println("No messages received\n");
////                return "No messages received";
//            }
        }
    }

    /**
     * Produce events to "myReadTopic".
//     * @param configFileName
     * @throws IOException
     * @throws InterruptedException
     */
    public void produce(String topicName, String data, JsonNode[] configArray) throws IOException, InterruptedException {
        // TODO: configFile will be passed using a list of key-values as JSON in the BOD of the request.
        for (JsonNode config : configArray) {
            String key = config.fieldNames().next();
            String value = config.get(key).asText();
            kafkaPros.setProperty(key, value);
        }

        var producer = new KafkaProducer<String, String>(kafkaPros);

//        final String topic = kafkaPros.getProperty(KafkaTopicConfig);
        /**
         * Send data to topic: topicName.
         */
        producer.send(new ProducerRecord<>(topicName, data), (recordMetadata, ex) -> {
            if (ex != null)
                ex.printStackTrace();
            else
                System.out.printf("Produced event to topic %s: data = %s.\n", topicName, data);
        });
    }
}

