package com.sanju.microservice_kafka_circuitbreaker.service;

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * Encapsulates the simulated external dependency call (e.g., a payment gateway).
 * Kept in its own Spring bean so that the @CircuitBreaker AOP proxy is correctly applied.
 * Self-invocation within the same bean bypasses Spring AOP, which is why this is extracted.
 */
@Slf4j
@Service
public class ExternalServiceClient {

    @CircuitBreaker(name = "externalService", fallbackMethod = "externalServiceFallback")
    public String callExternalService(String eventId) {
        log.info("[EXTERNAL] Attempting to call external service for event id={}", eventId);

        // Simulate an external service call that can fail ~50% of the time
        if (Math.random() < 0.5) {
            throw new RuntimeException("Simulated external service failure for event id=" + eventId);
        }

        // Simulate network/processing latency
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        log.info("[EXTERNAL] External service call succeeded for event id={}", eventId);
        return "Processed: " + eventId;
    }

    /**
     * Fallback triggered when:
     * - The circuit is OPEN (fast-fail, no actual call made)
     * - The circuit is HALF_OPEN and the test call fails
     * - A call fails normally
     */
    public String externalServiceFallback(String eventId, Throwable t) {
        log.warn("[CIRCUIT-BREAKER] Fallback activated for event id={}. Cause: {}", eventId, t.getMessage());
        return "Fallback for: " + eventId;
    }
}
