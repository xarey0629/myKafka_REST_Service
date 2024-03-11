package com.example.restservice;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

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

    MyKafkaServer myKafkaServer = new MyKafkaServer();

    @PostMapping("/readTopic/{topicName}")
    public String readTopic(@PathVariable String topicName, @RequestBody JsonNode[] configArray) throws IOException, InterruptedException, ExecutionException {
        // TODO
        return myKafkaServer.consume(topicName, configArray);
    }

    @PostMapping("/writeTopic/{topicName}/{data}")
    public void writeTopic(@PathVariable String topicName, @PathVariable String data, @RequestBody JsonNode[] configArray) throws IOException, InterruptedException, ExecutionException {
        // TODO
        myKafkaServer.produce(topicName, data, configArray);
    }

    @PostMapping("transformMessage/{readTopic}/{writeTopic}")
    public void transformMessage(@PathVariable String readTopic, @PathVariable String writeTopic, @RequestBody JsonNode[] configArray) throws IOException, InterruptedException, ExecutionException {
        // TODO
        String data = myKafkaServer.consume(readTopic, configArray);
        myKafkaServer.produce(writeTopic, data.toUpperCase(), configArray);
    }
}



