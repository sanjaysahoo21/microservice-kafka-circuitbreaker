package com.sanju.microservice_kafka_circuitbreaker.service;

import com.sanju.microservice_kafka_circuitbreaker.model.EventRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class EventConsumer {

    // Injected self-reference so that Spring AOP proxying is applied on callExternalService.
    // Direct this.callExternalService() would bypass the proxy and the @CircuitBreaker would never trigger.
    private final ExternalServiceClient externalServiceClient;

    @KafkaListener(topics = "${spring.kafka.topic.invoice-processing}")
    public void consumeEvent(EventRequest event) {
        log.info("[KAFKA] Received event id={} type={}", event.getId(), event.getType());
        try {
            String result = externalServiceClient.callExternalService(event.getId());
            log.info("[KAFKA] Successfully processed event id={} result='{}'", event.getId(), result);
        } catch (Exception e) {
            // After fallback is exhausted or circuit is OPEN and fallback itself throws,
            // we catch here to prevent the consumer thread from crashing.
            log.error("[KAFKA] Final failure processing event id={}. Error: {}", event.getId(), e.getMessage());
        }
    }
}
