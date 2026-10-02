package com.sanju.microservice_kafka_circuitbreaker.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanju.microservice_kafka_circuitbreaker.model.EventRequest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.verify;

/**
 * Integration test that verifies end-to-end message flow:
 * EventProducer -> Kafka topic -> EventConsumer -> ExternalServiceClient.
 * Requires Docker to be running (uses Testcontainers to spin up Kafka).
 */
@SpringBootTest
@Testcontainers
@DirtiesContext
public class KafkaIntegrationTest {

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.0"));

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @Autowired
    private EventProducer eventProducer;

    @SpyBean
    private EventConsumer eventConsumer;

    @SpyBean
    private ExternalServiceClient externalServiceClient;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    public void testEventPublishedToKafkaAndConsumed() throws Exception {
        EventRequest request = new EventRequest();
        request.setId(UUID.randomUUID().toString());
        request.setType("INVOICE_CREATED");
        request.setData(objectMapper.readTree("{\"amount\": 100.0}"));

        eventProducer.publishEvent(request);

        // Wait until the consumer receives the event (up to 10s)
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                verify(eventConsumer, atLeast(1)).consumeEvent(any(EventRequest.class))
        );
    }

    @Test
    public void testExternalServiceClientIsCalledDuringConsumption() throws Exception {
        EventRequest request = new EventRequest();
        request.setId(UUID.randomUUID().toString());
        request.setType("INVOICE_PAID");
        request.setData(objectMapper.readTree("{\"invoiceId\": \"INV-001\"}"));

        eventProducer.publishEvent(request);

        // The consumer should have called the external service client
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                verify(externalServiceClient, atLeast(1)).callExternalService(any(String.class))
        );
    }
}
