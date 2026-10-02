package com.sanju.microservice_kafka_circuitbreaker.controller;

import com.sanju.microservice_kafka_circuitbreaker.model.EventRequest;
import com.sanju.microservice_kafka_circuitbreaker.service.EventProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/events")
@RequiredArgsConstructor
public class EventController {

    private final EventProducer eventProducer;

    @PostMapping
    public ResponseEntity<Void> publishEvent(@RequestBody EventRequest request) {
        eventProducer.publishEvent(request);
        return ResponseEntity.accepted().build();
    }
}
