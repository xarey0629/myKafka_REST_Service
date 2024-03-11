package com.example.restservice;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.print.attribute.standard.Media;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

@RestController
public class Controllers {

    public Controllers() throws IOException {
    }

    @GetMapping("/uuid") // Map Get method to "/uuid"
    @ResponseBody
    public String uuid(){
        return "<h1>s2568786</h1>";
    }

    private String text = ""; // Store system data;

    @PostMapping("/writevalue") // Map Post method to "/writevalue"
    @ResponseBody
    public void writeValue(@RequestParam(name = "value") String value){
        text = value;
    }

    @GetMapping(value = "/readvalue", produces = "text/plain") // Map Get method to "/readvalue" and set it as text/plain
    @ResponseBody
    public String readValue(){
        return text;
    }

    @PostMapping(value = "/callservice", consumes = "application/json", produces = "application/json")
    public ResponseEntity<String> callService(@RequestBody Call call){
        String ext = call.getExternalBaseUrl();
        String params = call.getParameters();
        String url = ext;
        if(params != null) url += params;
        System.out.println("This is the URL:" + url);
        RestTemplate restTemplate = new RestTemplate();
        ResponseEntity<String> response = restTemplate.getForEntity(url, String.class);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
//        headers.setContentType(MediaType.parseMediaType(response.getHeaders().getContentType().toString()));

        MediaType contentType = response.getHeaders().getContentType();
        String type = contentType.getType();
        String subtype =contentType.getSubtype();
        System.out.println("Content-Type: " + contentType);
        System.out.println("Type: " + type);
        System.out.println("Subtype: " + subtype);

        return new ResponseEntity<>(response.getBody(), headers, response.getStatusCode());
    }

    MyProducer producer = new MyProducer();

    @PostMapping("/readTopic/{topicName}")
    public String readTopic(@PathVariable String topicName, @RequestBody JsonNode[] configArray) throws IOException, InterruptedException {
        // TODO
        return producer.consume(topicName, configArray);
    }

    @PostMapping("/writeTopic/{topicName}/{data}")
    public void writeTopic(@PathVariable String topicName, @PathVariable String data, @RequestBody JsonNode[] configArray) throws IOException, InterruptedException {
        // TODO
        producer.produce(topicName, data, configArray);
    }

}



