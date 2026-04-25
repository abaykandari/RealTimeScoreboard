# Real-Time Scoreboard — Java Spring Boot

A production-grade real-time leaderboard system rebuilt in **Java 21 + Spring Boot 3**, preserving the full architecture of the original Go system: **gRPC streaming**, **Kafka event pipeline**, and **Redis sorted sets**, with a bonus **WebSocket (STOMP)** channel for browser clients.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Project Structure](#project-structure)
3. [Component Deep-Dive](#component-deep-dive)
4. [Full Data Flow](#full-data-flow)
5. [How to Run](#how-to-run)
6. [API Reference](#api-reference)
7. [Configuration](#configuration)
8. [Testing](#testing)
9. [Scaling Strategies](#scaling-strategies)
10. [Design Decisions](#design-decisions)

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────┐
│  Clients                                                         │
│  ┌─────────────┐   ┌──────────────┐   ┌────────────────────┐     │
│  │ gRPC Client │   │ REST Client  │   │ Browser (WebSocket) │    │
│  └──────┬──────┘   └──────┬───────┘   └─────────┬──────────┘     │
└─────────┼────────────────┼──────────────────────┼────────────────┘
          │ gRPC :9090      │ HTTP :8080            │ WS :8080/ws
          ▼                 ▼                       ▼
┌─────────────────────────────────────────────────────────────────┐
│  Spring Boot Application                                        │
│                                                                 │
│  ┌───────────────────┐   ┌──────────────────────────────────┐   │
│  │ JWT Interceptor   │   │ Spring Security Filter Chain     │   │
│  │ (gRPC global)     │   │ (REST)                           │   │
│  └────────┬──────────┘   └──────────────────────────────────┘   │
│           │                                                     │
│  ┌────────▼──────────────────────────────────────────────────┐  │
│  │  Service Layer                                            │  │
│  │  ┌──────────────┐  ┌───────────────┐  ┌───────────────┐   │  │
│  │  │  AuthService │  │  ScoreService │  │LeaderboardSvc │   │  │
│  │  └──────────────┘  └───────┬───────┘  └───────────────┘   │  │
│  └────────────────────────────┼──────────────────────────────┘  │
│                               │                                 │
│  ┌────────────────────────────▼──────────┐                      │
│  │  Kafka Producer                       │                      │
│  │  Key = userId (partition ordering)    │                      │
│  └────────────────────────────┬──────────┘                      │
└───────────────────────────────┼─────────────────────────────────┘
                                │
              ┌─────────────────▼─────────────────────────┐
              │   Kafka Topic: leaderboard-score-events   │
              │   6 partitions · acks=all · idempotent    │
              └─────────────────┬─────────────────────────┘
                                │
┌───────────────────────────────▼────────────────────────────────┐
│  Spring Boot Application (consumer side)                       │
│                                                                │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  ScoreEventConsumer  (3 concurrent threads)             │   │
│  │  manual ACK · retry 3× · DLT on failure                 │   │
│  └────────────────────┬────────────────────────────────────┘   │
│                       │                                        │
│            ┌──────────▼───────────────────────────────┐        │
│            │  LeaderboardService (Redis writes)       │        │
│            │  ZADD leaderboard:global GT score userId │        │
│            │  ZADD leaderboard:game:{id} GT score ... │        │
│            └──────────┬───────────────────────────────┘        │
│                       │                                        │
│            ┌──────────▼───────────────────────────────┐        │
│            │  LeaderboardStreamBroadcaster            │        │
│            │  → gRPC observers (CopyOnWriteArrayList) │        │
│            │  → WebSocket /topic/leaderboard/{gameId} │        │
│            └──────────────────────────────────────────┘        │
└────────────────────────────────────────────────────────────────┘
                       │
              ┌────────▼────────┐
              │  Redis (ZSETs)  │
              │  O(log N) R/W   │
              └─────────────────┘
```

### Technology Stack

| Layer | Technology | Why |
|---|---|---|
| Language | Java 21 | Virtual threads, records, sealed types |
| Framework | Spring Boot 3.2 | Auto-configuration, actuator, validation |
| RPC | gRPC + grpc-spring-boot-starter | Binary protocol, server-streaming, bi-di |
| Events | Apache Kafka (Spring Kafka) | Durable, ordered, partitioned event log |
| State | Redis 7 (Lettuce pool) | O(log N) sorted sets, sub-ms latency |
| Auth | JWT (JJWT 0.12) + BCrypt | Stateless, no session storage needed |
| Real-time (bonus) | STOMP over WebSocket | Browser-native, no gRPC-web proxy needed |
| Metrics | Micrometer + Prometheus | Standard, plugs into Grafana |

---

## Project Structure

```
real-time-scoreboard/
│
├── src/
│   ├── main/
│   │   ├── proto/
│   │   │   └── leaderboard.proto          # gRPC service + message definitions
│   │   │
│   │   ├── java/com/scoreboard/
│   │   │   ├── ScoreboardApplication.java  # Entry point
│   │   │   │
│   │   │   ├── config/
│   │   │   │   ├── KafkaConfig.java        # Producer/consumer factories, topics
│   │   │   │   ├── RedisConfig.java        # Lettuce pool, RedisTemplate beans
│   │   │   │   ├── AsyncConfig.java        # Thread pools (gRPC stream, Kafka proc)
│   │   │   │   ├── SecurityConfig.java     # JWT filter chain, BCrypt bean
│   │   │   │   └── WebSocketConfig.java    # STOMP broker, /ws endpoint
│   │   │   │
│   │   │   ├── model/
│   │   │   │   ├── ScoreEvent.java         # Kafka message (producer & consumer)
│   │   │   │   ├── UserProfile.java        # Redis-stored user data
│   │   │   │   └── LeaderboardEntry.java   # Service-layer DTO
│   │   │   │
│   │   │   ├── kafka/
│   │   │   │   ├── ScoreEventProducer.java # Async Kafka publish with callbacks
│   │   │   │   └── ScoreEventConsumer.java # Manual-ack consumer, DLT handler
│   │   │   │
│   │   │   ├── service/
│   │   │   │   ├── LeaderboardService.java       # All Redis ZSET operations
│   │   │   │   ├── ScoreService.java              # Orchestrates submit → Kafka
│   │   │   │   ├── AuthService.java               # Register / login logic
│   │   │   │   └── LeaderboardStreamBroadcaster.java # Fan-out to gRPC + WS
│   │   │   │
│   │   │   ├── grpc/
│   │   │   │   └── LeaderboardGrpcService.java   # All RPC implementations
│   │   │   │
│   │   │   ├── controller/
│   │   │   │   ├── AuthController.java            # POST /api/auth/*
│   │   │   │   ├── LeaderboardController.java     # GET/POST /api/*
│   │   │   │   └── GlobalExceptionHandler.java    # @RestControllerAdvice
│   │   │   │
│   │   │   ├── security/
│   │   │   │   ├── JwtUtil.java                   # Sign / validate tokens
│   │   │   │   └── JwtServerInterceptor.java      # gRPC global auth interceptor
│   │   │   │
│   │   │   └── scheduler/
│   │   │       └── PeriodicLeaderboardScheduler.java  # Heartbeat broadcaster
│   │   │
│   │   └── resources/
│   │       └── application.yml
│   │
│   └── test/
│       ├── java/com/scoreboard/
│       │   ├── ScoreboardIntegrationTest.java    # EmbeddedKafka E2E test
│       │   └── service/
│       │       └── LeaderboardServiceTest.java   # Unit tests with Mockito
│       └── resources/
│           └── application-test.yml
│
├── scripts/
│   └── demo_client.py           # Python gRPC demo client
│
├── Dockerfile                   # Multi-stage: Maven build → JRE 21 runtime
├── docker-compose.yml           # Zookeeper, Kafka, Kafka UI, Redis, Redis UI, App
├── pom.xml
└── README.md
```

---

## Component Deep-Dive

### 1. Proto / gRPC Layer (`leaderboard.proto` + `LeaderboardGrpcService`)

The service exposes **7 RPCs**:

| RPC | Type | Auth Required |
|---|---|---|
| `Register` | Unary | ❌ Public |
| `Login` | Unary | ❌ Public |
| `SubmitScore` | Unary | ✅ JWT |
| `GetGlobalLeaderboard` | Unary | ✅ JWT |
| `GetGameLeaderboard` | Unary | ✅ JWT |
| `GetUserRank` | Unary | ✅ JWT |
| `StreamLeaderboard` | **Server-streaming** | ✅ JWT |

`StreamLeaderboard` keeps the HTTP/2 stream open and pushes a `LeaderboardUpdate` message every time the Kafka consumer processes a new score (event-driven) **plus** a periodic heartbeat every 2 s so clients always see fresh data even during quiet periods.

**Auth** is enforced by `JwtServerInterceptor` — a `@GrpcGlobalServerInterceptor` bean that runs before every RPC call, extracts the `Authorization: Bearer <token>` metadata header, validates it, and injects the `userId` into the gRPC `Context`.

---

### 2. Kafka Pipeline

```
SubmitScore RPC
    │
    ▼
ScoreEventProducer.publishScoreEvent(event)
    │  Key = userId  (partition ordering: all events for user X → same partition)
    │  Value = ScoreEvent JSON
    │  acks = all · idempotent · snappy compressed
    ▼
[Kafka topic: leaderboard-score-events, 6 partitions]
    │
    ▼
ScoreEventConsumer.onScoreEvent(record, ack)
    │  3 concurrent consumer threads
    │  Manual acknowledgement (AckMode.MANUAL_IMMEDIATE)
    │  On failure: retry 3× with 1 s backoff → DLT
    ▼
LeaderboardService.updateScore(event)     ← Redis write
    ▼
LeaderboardStreamBroadcaster.broadcastUpdate(gameId)
    ├── gRPC stream observers (push LeaderboardUpdate)
    └── WebSocket /topic/leaderboard/{gameId} (push JSON payload)
    ▼
ack.acknowledge()   ← Kafka offset committed only on full success
```

**Why manual ACK?**  
If the Redis write or broadcast fails, we do not commit the Kafka offset. The consumer retries the same record. This gives **at-least-once** processing — the leaderboard will never silently drop a score update.

---

### 3. Redis Leaderboard (`LeaderboardService`)

Every score update touches two sorted sets:

```
ZADD leaderboard:global       GT <score> <userId>
ZADD leaderboard:game:<id>    GT <score> <userId>
HSET leaderboard:global:meta  <userId> <username>
HSET leaderboard:game:<id>:meta <userId> <username>
```

The `GT` flag (implemented as an atomic Lua script) ensures only the **personal best** is stored — submitting a lower score never downgrades a user's rank.

Reading the top-10:

```
ZREVRANGE leaderboard:global 0 9 WITHSCORES   → O(log N + 10)
```

Getting a specific user's rank:

```
ZREVRANK leaderboard:global <userId>           → O(log N)
ZSCORE   leaderboard:global <userId>           → O(1)
```

---

### 4. WebSocket (Bonus Channel)

Browser clients connect via SockJS / STOMP:

```javascript
const client = new StompClient({ webSocketFactory: () => new SockJS('/ws') });
client.activate();
client.onConnect = () => {
  client.subscribe('/topic/leaderboard/chess-championship', (msg) => {
    const update = JSON.parse(msg.body);
    console.log(update.entries);   // LeaderboardEntry[]
  });
};
```

The same `LeaderboardStreamBroadcaster` that drives gRPC streams also calls `messagingTemplate.convertAndSend(destination, payload)` after each Kafka event — zero code duplication.

---

## Full Data Flow

```
① Player submits score via gRPC:
   SubmitScoreRequest { userId, username, gameId, score=9500 }

② JwtServerInterceptor validates bearer token → injects userId into Context

③ LeaderboardGrpcService.submitScore() calls ScoreService.submitScore()

④ ScoreService builds a ScoreEvent and calls ScoreEventProducer.publishScoreEvent()
   → Kafka record published with key=userId to leaderboard-score-events partition 3

⑤ ScoreEventConsumer (thread-2) picks up the record from partition 3

⑥ LeaderboardService.updateScore() runs Lua script:
   ZADD leaderboard:global GT 9500 "user-001"    (only updates if 9500 > current)
   ZADD leaderboard:game:chess GT 9500 "user-001"

⑦ LeaderboardStreamBroadcaster.broadcastUpdate("chess") fires:
   a. Fetches ZREVRANGE leaderboard:game:chess 0 9 WITHSCORES from Redis
   b. Calls observer.onNext(LeaderboardUpdate{...}) for each active gRPC stream
   c. Calls messagingTemplate.convertAndSend("/topic/leaderboard/chess", payload)

⑧ ack.acknowledge() commits Kafka offset for partition 3, offset 42

⑨ All connected gRPC streaming clients and WebSocket clients receive the update
   within ~5–20 ms of the original score submission.
```

---

## How to Run

### Prerequisites

- Docker & Docker Compose (for infrastructure)
- Java 21 + Maven 3.9 (for local build)
- `protoc` is **not** required manually — Maven plugin handles code generation

---

### Option A — Full Docker Stack (recommended)

```bash
# 1. Clone the project
git clone <repo-url>
cd real-time-scoreboard

# 2. Build the application JAR (skip tests for speed)
mvn clean package -DskipTests

# 3. Start everything: Zookeeper, Kafka, Redis, App, UI tools
docker-compose up --build -d

# 4. Check all containers are healthy
docker-compose ps

# 5. Tail application logs
docker-compose logs -f app
```

Services after startup:

| Service | URL |
|---|---|
| REST API | http://localhost:8080 |
| gRPC | localhost:9090 |
| WebSocket | ws://localhost:8080/ws |
| Actuator / Health | http://localhost:8080/actuator/health |
| Kafka UI | http://localhost:8090 |
| Redis Commander | http://localhost:8091 |

---

### Option B — Run Locally (infrastructure via Docker, app via Maven)

```bash
# 1. Start only infrastructure
docker-compose up -d zookeeper kafka redis

# 2. Wait ~20 seconds for Kafka to be ready, then run the app
mvn spring-boot:run \
  -Dspring-boot.run.jvmArguments="-DKAFKA_BROKERS=localhost:9092 -DREDIS_HOST=localhost"

# App starts on port 8080 (REST) and 9090 (gRPC)
```

---

### Option C — Run the Demo Client

```bash
# Install Python gRPC tools
pip install grpcio grpcio-tools

# Generate Python stubs from proto file
python -m grpc_tools.protoc \
  -I src/main/proto \
  --python_out=scripts \
  --grpc_python_out=scripts \
  src/main/proto/leaderboard.proto

# Run the demo (registers players, submits scores, opens stream)
cd scripts && python demo_client.py
```

Expected output:
```
=======================================================
  Real-Time Scoreboard — gRPC Demo Client
=======================================================
[Register] userId=a3f2...  message=Registration successful
[Register] userId=b1c4...  message=Registration successful
[Login]    token=eyJhbGci...  userId=a3f2...
[Score]    success=True  rank=1  score=8234
[Stream]   Listening for leaderboard updates (game='chess-championship')...

  ── Update @ 1714000123456 ─────────────────
    #1   alice            score=8234
    #2   charlie          score=7100
    #3   bob              score=5900
```

---

## API Reference

### REST Endpoints

#### Auth (public — no token required)

```
POST /api/auth/register
Content-Type: application/json

{
  "username": "alice",
  "password": "securepassword",
  "email": "alice@example.com"
}

→ 201 { "userId": "...", "message": "Registration successful" }
```

```
POST /api/auth/login
Content-Type: application/json

{ "username": "alice", "password": "securepassword" }

→ 200 { "token": "eyJ...", "userId": "..." }
```

#### Leaderboard (JWT required)

```
GET /api/leaderboard/global?limit=10&offset=0
Authorization: Bearer <token>

→ 200 {
    "entries": [
      { "rank": 1, "userId": "...", "username": "alice", "score": 9500 },
      ...
    ],
    "total": 1523
  }
```

```
GET /api/leaderboard/game/chess-championship?limit=5
Authorization: Bearer <token>
```

```
GET /api/leaderboard/rank/{userId}?gameId=chess-championship
Authorization: Bearer <token>

→ 200 { "rank": 3, "userId": "...", "username": "bob", "score": 7800 }
```

#### Score Submission

```
POST /api/scores
Authorization: Bearer <token>
Content-Type: application/json

{
  "userId": "...",
  "username": "alice",
  "gameId": "chess-championship",
  "score": 9500
}

→ 202 {
    "accepted": true,
    "currentRank": 1,
    "message": "Score queued — leaderboard will update shortly"
  }
```

---

### gRPC (see proto file for full schema)

```protobuf
service LeaderboardService {
  rpc Register(RegisterRequest)               returns (RegisterResponse);
  rpc Login(LoginRequest)                     returns (LoginResponse);
  rpc SubmitScore(SubmitScoreRequest)         returns (SubmitScoreResponse);
  rpc GetGlobalLeaderboard(GetLeaderboardRequest) returns (LeaderboardResponse);
  rpc GetGameLeaderboard(GetLeaderboardRequest)   returns (LeaderboardResponse);
  rpc GetUserRank(GetUserRankRequest)         returns (GetUserRankResponse);
  rpc StreamLeaderboard(StreamLeaderboardRequest) returns (stream LeaderboardUpdate);
}
```

---

### WebSocket (STOMP)

```javascript
// Connect
const client = new StompClient({ webSocketFactory: () => new SockJS('/ws') });
client.activate();

// Subscribe to a game board
client.subscribe('/topic/leaderboard/chess-championship', (msg) => {
  const { gameId, entries, timestamp } = JSON.parse(msg.body);
});

// Subscribe to the global board
client.subscribe('/topic/leaderboard/global', (msg) => { ... });
```

---

## Configuration

All settings are in `src/main/resources/application.yml` and can be overridden by environment variables:

| Environment Variable | Default | Description |
|---|---|---|
| `KAFKA_BROKERS` | `localhost:9092` | Kafka bootstrap servers |
| `REDIS_HOST` | `localhost` | Redis hostname |
| `REDIS_PORT` | `6379` | Redis port |
| `REDIS_PASSWORD` | _(empty)_ | Redis password (if AUTH enabled) |
| `JWT_SECRET` | _(must set)_ | HS256 signing key (≥32 chars) |
| `SERVER_PORT` | `8080` | REST / WebSocket port |
| `GRPC_SERVER_PORT` | `9090` | gRPC port |

---

## Testing

```bash
# Unit tests only (no Docker required)
mvn test

# Integration tests (requires Docker containers running)
docker-compose up -d zookeeper kafka redis
mvn test -Dspring.profiles.active=test

# Test coverage report
mvn jacoco:report
open target/site/jacoco/index.html
```

The integration tests use `@EmbeddedKafka` — a real in-process Kafka broker — so no external Docker is needed for the Kafka layer in CI.

---

## Scaling Strategies

### Horizontal Scaling (multiple app instances)

| Concern | Solution |
|---|---|
| Kafka consumers | Increase `partitions` to match instance count; Kafka auto-rebalances |
| Stateless gRPC | Put an L4 load balancer (NGINX / Envoy) in front; all state is in Redis |
| WebSocket | Replace in-memory STOMP broker with a RabbitMQ STOMP bridge; all nodes share the same exchange |
| Scheduler | Use ShedLock (`@SchedulerLock`) to elect one leader for the heartbeat job |

### Redis Scaling

- **Redis Cluster** (sharding): shard by `gameId` so each node owns a subset of leaderboards
- **Read replicas**: serve `ZRANGE` queries from replicas, writes always go to primary
- **Redis Sentinel**: automatic failover with no application changes needed

### Kafka Scaling

- Increase partition count to match peak consumer throughput
- Add brokers to the cluster and rebalance replicas
- Use Kafka Streams for complex aggregations (e.g. rolling 24-hour leaderboard)

### gRPC at Scale

- Enable **TLS** (`grpc.server.security.enabled=true`) and load-balance at L7 with Envoy
- Use **gRPC-web** proxy (Envoy) to serve browser clients directly over gRPC instead of WebSocket

---

## Design Decisions

**Why manual Kafka ACK?**  
Automatic commit can mark a record as "processed" before the Redis write succeeds. Manual ACK guarantees the offset is only committed after the full pipeline (Redis + broadcast) completes — at-least-once semantics with no silent data loss.

**Why Lua script for ZADD GT?**  
Redis 6.2+ supports `ZADD GT` natively, but the Spring `RedisTemplate` wrapper does not expose it directly. A short Lua script gives the same atomic compare-and-update that is guaranteed to not race with concurrent writes to the same key.

**Why Lettuce over Jedis?**  
Lettuce uses Netty under the hood and supports non-blocking I/O. All Redis commands from Kafka consumer threads use a shared connection pool without thread contention, which is critical under load.

**Why both gRPC streaming AND WebSocket?**  
gRPC streaming is optimal for native clients (mobile, CLI, backend-to-backend). WebSocket / STOMP is necessary for browser clients where gRPC-web adds proxy complexity. Both channels are driven by the same broadcaster — zero duplication.

**Why store users in Redis (not a database)?**  
For a leaderboard system the hot read path is always the sorted set. Introducing a relational database just for user profiles adds operational complexity. Redis with a 24-hour TTL refresh on login is sufficient; a secondary HSET index `username→userId` makes login O(1) at scale.
