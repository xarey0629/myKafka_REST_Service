package com.example.restservice;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

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
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class MyKafkaServer {
    Properties kafkaPros;
    MyKafkaServer() throws IOException {
        this.kafkaPros = MyKafkaServer.loadConfig("configfile");
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

    public String consume(String topicName, JsonNode[] configArray) throws IOException, InterruptedException, ExecutionException {
        for (JsonNode config : configArray) {
            String key = config.fieldNames().next();
            String value = config.get(key).asText();
            kafkaPros.setProperty(key, value);
        }

        try{
            // Check does topic exist.
            AdminClient admin = AdminClient.create(kafkaPros);
            ListTopicsResult listTopics = admin.listTopics();
            Set<String> names = listTopics.names().get();
            boolean contains = names.contains(topicName);
            if (!contains) {
                throw new TopicNotFoundException("Topic doesn't exist: " + topicName);
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
            }
        }catch (Exception e){
            throw new TopicNotFoundException("User error found.");
        }
    }

    /**
     * Produce events to "myReadTopic".
//     * @param configFileName
     * @throws IOException
     * @throws InterruptedException
     */
    public void produce(String topicName, String data, JsonNode[] configArray) throws IOException, InterruptedException, ExecutionException {
        // TODO: configFile will be passed using a list of key-values as JSON in the BOD of the request.
        for (JsonNode config : configArray) {
            String key = config.fieldNames().next();
            String value = config.get(key).asText();
            kafkaPros.setProperty(key, value);
        }

        try{
            // Check does topic exist.
            AdminClient admin = AdminClient.create(kafkaPros);
            ListTopicsResult listTopics = admin.listTopics();
            Set<String> names = listTopics.names().get();
            boolean contains = names.contains(topicName);
            if (!contains) {
                throw new TopicNotFoundException("User error found.");
            }
            String key = "S2568786";
            var producer = new KafkaProducer<String, String>(kafkaPros);
            producer.send(new ProducerRecord<>(topicName, key, data), (recordMetadata, ex) -> {
                if (ex != null){
                    ex.printStackTrace();
                }
                else
                    System.out.printf("Produced event to topic %s: key = %s, data = %s.\n", topicName, key, data);
            });
        }catch (Exception e){
            throw new TopicNotFoundException("User error found.");
        }
    }

}

