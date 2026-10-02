package com.sanju.microservice_kafka_circuitbreaker.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanju.microservice_kafka_circuitbreaker.model.EventRequest;
import com.sanju.microservice_kafka_circuitbreaker.service.EventProducer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Unit tests for EventController, verifying HTTP contract without starting a full application context.
 */
@WebMvcTest(EventController.class)
public class EventControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private EventProducer eventProducer;

    @Test
    public void testPublishEvent_returns202Accepted() throws Exception {
        EventRequest request = new EventRequest();
        request.setId("test-id-123");
        request.setType("INVOICE_CREATED");
        request.setData(objectMapper.readTree("{\"amount\": 500.0, \"currency\": \"USD\"}"));

        mockMvc.perform(post("/api/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isAccepted()); // 202

        verify(eventProducer, times(1)).publishEvent(any(EventRequest.class));
    }

    @Test
    public void testPublishEvent_withMissingFields_returns202Accepted() throws Exception {
        // API is lenient - partial payload is still accepted and forwarded
        String partialPayload = "{\"id\": \"partial-id\", \"type\": \"TEST_EVENT\"}";

        mockMvc.perform(post("/api/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(partialPayload))
                .andExpect(status().isAccepted());

        verify(eventProducer, times(1)).publishEvent(any(EventRequest.class));
    }

    @Test
    public void testPublishEvent_withFullPayload_invokesProducerOnce() throws Exception {
        doNothing().when(eventProducer).publishEvent(any());

        String payload = """
                {
                  "id": "invoice-99",
                  "type": "INVOICE_PAID",
                  "data": {
                    "invoiceId": "INV-099",
                    "amount": 1250.00,
                    "paidAt": "2026-09-30T17:00:00Z"
                  }
                }
                """;

        mockMvc.perform(post("/api/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(payload))
                .andExpect(status().isAccepted());

        verify(eventProducer, times(1)).publishEvent(any(EventRequest.class));
    }
}
