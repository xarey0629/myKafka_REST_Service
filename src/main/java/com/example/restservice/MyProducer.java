package com.example.restservice;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.Random;

public class MyProducer {
//    String topicName = null;
//    String data = null;

    MyProducer(){
//        this.topicName = topicName;
//        this.data = data;
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
//    public final String KafkaTopicConfig = "kafka.topic";

    /**
     * Produce events to "myReadTopic".
//     * @param configFileName
     * @throws IOException
     * @throws InterruptedException
     */
    public void produce(String topicName, String data) throws IOException, InterruptedException {
        // TODO: configFile will be passed using a list of key-values as JSON in the BOD of the request.
        Properties kafkaPros = MyProducer.loadConfig("/Users/edlin/Desktop/修課/UoE_Sem2/ACP/CW2_Program/acp_submission_2/configfile");

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

