package com.sanju.microservice_kafka_circuitbreaker.config;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class Resilience4jConfig {

    private final CircuitBreakerRegistry circuitBreakerRegistry;

    @PostConstruct
    public void setupEventPublisher() {
        CircuitBreaker circuitBreaker = circuitBreakerRegistry.circuitBreaker("externalService");
        circuitBreaker.getEventPublisher()
            .onStateTransition(event -> log.info("Circuit Breaker '{}' transitioned from {} to {}", 
                    event.getCircuitBreakerName(), 
                    event.getStateTransition().getFromState(), 
                    event.getStateTransition().getToState()));
    }
}
