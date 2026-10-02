package com.sanju.microservice_kafka_circuitbreaker.service;

import com.sanju.microservice_kafka_circuitbreaker.model.EventRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class EventProducer {

    private final KafkaTemplate<String, EventRequest> kafkaTemplate;

    @Value("${spring.kafka.topic.invoice-processing}")
    private String topicName;

    public void publishEvent(EventRequest event) {
        log.info("Publishing event with id: {} to topic: {}", event.getId(), topicName);
        kafkaTemplate.send(topicName, event.getId(), event)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("Successfully published event with id: {} and offset: {}",
                                event.getId(), result.getRecordMetadata().offset());
                    } else {
                        log.error("Failed to publish event with id: {}. Error: {}", event.getId(), ex.getMessage());
                    }
                });
    }
}
