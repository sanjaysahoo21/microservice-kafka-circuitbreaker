# 🔄 Microservice Kafka Circuit Breaker

A production-ready, event-driven microservice built with **Java 21 / Spring Boot 3** that demonstrates:
- ✅ Asynchronous communication via **Apache Kafka**
- ✅ Fault tolerance via the **Circuit Breaker pattern** (Resilience4j)
- ✅ Clean architecture with layered separation of concerns
- ✅ Full Docker-based local environment with a single command

---

## 📐 Architecture Overview

```
┌──────────────────────────────────────────────────┐
│                   REST Client                    │
│           POST /api/events (JSON)                │
└───────────────────┬──────────────────────────────┘
                    │ HTTP 202 Accepted
                    ▼
┌──────────────────────────────────────────────────┐
│              EventController                     │
│         (Spring MVC REST Controller)             │
└───────────────────┬──────────────────────────────┘
                    │
                    ▼
┌──────────────────────────────────────────────────┐
│              EventProducer (Service)             │
│        Publishes to Kafka via KafkaTemplate      │
└───────────────────┬──────────────────────────────┘
                    │
                    ▼
┌──────────────────────────────────────────────────┐
│        Kafka Topic: invoice_processing_events    │
└───────────────────┬──────────────────────────────┘
                    │
                    ▼
┌──────────────────────────────────────────────────┐
│           EventConsumer (Service)                │
│        @KafkaListener subscriber                 │
└───────────────────┬──────────────────────────────┘
                    │
                    ▼
┌──────────────────────────────────────────────────┐
│        ExternalServiceClient (Service)           │
│  @CircuitBreaker(name = "externalService")       │
│  Simulates a downstream payment gateway call     │
│                                                  │
│  ┌─────────┐   failure rate   ┌──────────────┐  │
│  │ CLOSED  │ ────────────────▶│   OPEN       │  │
│  └─────────┘  >= 50% (10 calls)└──────┬───────┘  │
│       ▲                               │ 10s wait  │
│       │ success                       ▼           │
│  ┌────┴────────┐             ┌──────────────┐    │
│  │ HALF_OPEN   │◀────────────│  (auto tran) │    │
│  └─────────────┘             └──────────────┘    │
└──────────────────────────────────────────────────┘
```

> **Key architectural decision**: `ExternalServiceClient` is a separate Spring bean from `EventConsumer`. This is intentional — Spring AOP proxies only intercept calls *between* beans. A self-call (`this.callExternalService()`) inside the same class would bypass the proxy, rendering the `@CircuitBreaker` annotation ineffective.

---

## 📁 Project Structure

```
microservice-kafka-circuitbreaker/
├── src/
│   ├── main/
│   │   ├── java/com/sanju/microservice_kafka_circuitbreaker/
│   │   │   ├── controller/
│   │   │   │   └── EventController.java        # POST /api/events
│   │   │   ├── service/
│   │   │   │   ├── EventProducer.java          # Kafka producer
│   │   │   │   ├── EventConsumer.java          # Kafka listener
│   │   │   │   └── ExternalServiceClient.java  # Circuit-breaker-protected call
│   │   │   ├── config/
│   │   │   │   ├── KafkaConfig.java            # Topic definition
│   │   │   │   └── Resilience4jConfig.java     # CB event logging
│   │   │   ├── model/
│   │   │   │   └── EventRequest.java           # DTO
│   │   │   └── MicroserviceKafkaCircuitbreakerApplication.java
│   │   └── resources/
│   │       └── application.yml
│   └── test/
│       └── java/com/sanju/microservice_kafka_circuitbreaker/
│           ├── controller/
│           │   └── EventControllerTest.java    # @WebMvcTest API tests
│           └── service/
│               ├── CircuitBreakerTest.java     # State transition tests
│               └── KafkaIntegrationTest.java   # Testcontainers integration tests
├── Dockerfile
├── docker-compose.yml
├── .env.example
└── pom.xml
```

---

## 🚀 Setup & Run

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (running)
- Java 21 (only for local builds without Docker)

### Start everything with one command

```bash
docker-compose up -d
```

This starts:
| Service      | Port  | Description                     |
|-------------|-------|---------------------------------|
| Zookeeper    | 2181  | Kafka coordination              |
| Kafka        | 9092  | Internal (container-to-container) |
| Kafka        | 9093  | External (host access)          |
| app-service  | 8080  | The Spring Boot microservice    |

### Check health

```bash
curl http://localhost:8080/actuator/health
```

Expected response:
```json
{
  "status": "UP",
  "components": {
    "circuitBreakers": { "status": "UP" },
    "kafka": { "status": "UP" }
  }
}
```

### View logs

```bash
docker-compose logs -f app-service
```

---

## 📡 API Documentation

### `POST /api/events`

Publishes a structured event to the `invoice_processing_events` Kafka topic.

**Request Body:**
```json
{
  "id": "string",
  "type": "string",
  "data": { /* any valid JSON object */ }
}
```

**Response:** `202 Accepted` (empty body)

#### cURL Examples

```bash
# Publish an INVOICE_CREATED event
curl -X POST http://localhost:8080/api/events \
  -H "Content-Type: application/json" \
  -d '{
    "id": "inv-001",
    "type": "INVOICE_CREATED",
    "data": {
      "customerId": "CUST-42",
      "amount": 250.00,
      "currency": "USD"
    }
  }'
```

```bash
# Publish multiple events to trigger Circuit Breaker OPEN state
for i in {1..15}; do
  curl -s -X POST http://localhost:8080/api/events \
    -H "Content-Type: application/json" \
    -d "{\"id\": \"inv-$i\", \"type\": \"INVOICE_PAID\", \"data\": {\"amount\": $i}}"
  echo ""
done
```

---

## ⚡ Kafka Integration

| Parameter             | Value                          |
|----------------------|--------------------------------|
| Topic                 | `invoice_processing_events`    |
| Producer Serializer   | `JsonSerializer`               |
| Consumer Deserializer | `JsonDeserializer`             |
| Consumer Group ID     | `invoice-processing-group`     |
| Auto-offset Reset     | `earliest`                     |

**Message Flow:**
1. Client sends `POST /api/events`
2. `EventController` delegates to `EventProducer`
3. `EventProducer` uses `KafkaTemplate` to publish to `invoice_processing_events`
4. `EventConsumer` receives the message via `@KafkaListener`
5. `EventConsumer` calls `ExternalServiceClient.callExternalService()` (circuit-breaker-protected)
6. On success: logs the result; on failure or open circuit: fallback response is logged

---

## 🔌 Circuit Breaker Implementation

Library: **Resilience4j** (`resilience4j-spring-boot3`)

### Parameters

| Parameter                          | Default | Env Variable                                      |
|------------------------------------|---------|---------------------------------------------------|
| `slidingWindowSize`                | 10      | —                                                 |
| `minimumNumberOfCalls`             | 5       | —                                                 |
| `failureRateThreshold`             | 50%     | `CIRCUIT_BREAKER_FAILURE_RATE_THRESHOLD`          |
| `waitDurationInOpenState`          | 10s     | `CIRCUIT_BREAKER_WAIT_DURATION_IN_OPEN_STATE_SECONDS` |
| `permittedNumberOfCallsInHalfOpenState` | 1  | `CIRCUIT_BREAKER_PERMITTED_CALLS_IN_HALF_OPEN_STATE` |
| `automaticTransitionFromOpenToHalfOpenEnabled` | true | — |

### State Transitions

```
CLOSED ──[failure rate >= 50% over 10 calls]──▶ OPEN
OPEN   ──[10 seconds elapsed]─────────────────▶ HALF_OPEN
HALF_OPEN ──[1 test call succeeds]────────────▶ CLOSED
HALF_OPEN ──[1 test call fails]───────────────▶ OPEN
```

### Observing State Transitions

Look for lines like these in `docker-compose logs -f app-service`:

```
INFO  [CIRCUIT-BREAKER] Circuit Breaker 'externalService' transitioned from CLOSED to OPEN
WARN  [CIRCUIT-BREAKER] Fallback activated for event id=inv-005. Cause: CircuitBreaker 'externalService' is OPEN...
INFO  [CIRCUIT-BREAKER] Circuit Breaker 'externalService' transitioned from OPEN to HALF_OPEN
INFO  [CIRCUIT-BREAKER] Circuit Breaker 'externalService' transitioned from HALF_OPEN to CLOSED
```

The `/actuator/health` endpoint also exposes real-time circuit breaker state:

```bash
curl http://localhost:8080/actuator/health | python -m json.tool
```

---

## 🧪 Testing

### Run all tests (requires Docker for integration tests)

```bash
./mvnw test
```

### Run only unit tests (no Docker needed)

```bash
./mvnw test -Dgroups="unit"
# or skip Testcontainers-based tests:
./mvnw test -Dtest="CircuitBreakerTest,EventControllerTest"
```

### Build without tests

```bash
./mvnw clean package -DskipTests
```

### Test Coverage

| Test Class               | Type        | What It Tests                                              |
|--------------------------|-------------|------------------------------------------------------------|
| `EventControllerTest`    | Unit (Slice)| HTTP 202 response, producer invocation, request parsing    |
| `CircuitBreakerTest`     | Unit        | CLOSED→OPEN transitions, fallback, HALF_OPEN, config values|
| `KafkaIntegrationTest`   | Integration | Full message flow via Testcontainers Kafka                 |

---

## 🔧 Environment Variables

See [`.env.example`](.env.example) for all configurable variables:

```env
SPRING_KAFKA_BOOTSTRAP_SERVERS=localhost:9093
CIRCUIT_BREAKER_FAILURE_RATE_THRESHOLD=50
CIRCUIT_BREAKER_WAIT_DURATION_IN_OPEN_STATE_SECONDS=10
CIRCUIT_BREAKER_PERMITTED_CALLS_IN_HALF_OPEN_STATE=1
```

---

## 🏗️ Architectural Decisions

| Decision | Rationale |
|---------|-----------|
| **Separate `ExternalServiceClient` bean** | Spring AOP proxies don't intercept self-invocations. Moving `@CircuitBreaker` to a separate bean ensures the proxy correctly wraps the method. |
| **`JsonSerializer` / `JsonDeserializer` for Kafka** | Enables structured, type-safe message payloads with flexible nested JSON via Jackson's `JsonNode`. |
| **`automaticTransitionFromOpenToHalfOpenEnabled: true`** | The circuit breaker automatically moves from OPEN → HALF_OPEN after the wait duration, enabling automatic recovery without manual intervention. |
| **`spring.json.trusted.packages: "*"`** | Simplifies deserialization in a single-service dev context. In production this should be scoped to specific packages. |
| **Multi-stage Dockerfile** | Build stage uses JDK; runtime stage uses lean JRE Alpine image, reducing the final image size significantly. |

---

## 🐳 Docker Commands

```bash
# Start all services
docker-compose up -d

# View real-time logs
docker-compose logs -f app-service

# Stop all services
docker-compose down

# Rebuild and restart the app after code changes
docker-compose up -d --build app-service
```
