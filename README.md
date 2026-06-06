<div align="center">

# 🏆 Real-Time Scoreboard

**A production-grade leaderboard system with live score streaming**

Built with Spring Boot 3 · gRPC · Apache Kafka · Redis · WebSocket

---

![Java](https://img.shields.io/badge/Java-21-orange?style=flat-square&logo=openjdk)
![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.2.5-brightgreen?style=flat-square&logo=springboot)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-7.6.0-black?style=flat-square&logo=apachekafka)
![Redis](https://img.shields.io/badge/Redis-7.2-red?style=flat-square&logo=redis)
![gRPC](https://img.shields.io/badge/gRPC-1.61.0-blue?style=flat-square&logo=grpc)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker)
![License](https://img.shields.io/badge/License-MIT-yellow?style=flat-square)

</div>

---

## What This Is

A backend service that handles real-time score updates for multiple concurrent games. Players submit scores via REST or gRPC; the system processes them through a Kafka pipeline, persists ranks in Redis, and instantly pushes live leaderboard updates to every connected client — over both gRPC server-streaming and WebSocket/STOMP.

Designed to be horizontally scalable, resilient to partial failures, and observable in production.

---

## Architecture

```
 Clients (Browser · Mobile · gRPC)
        │ REST / WebSocket              │ gRPC (port 9090)
        ▼                               ▼
 ┌──────────────────────────────────────────────┐
 │          Spring Boot Application             │
 │                                              │
 │   REST Controllers   ·   gRPC Service        │
 │   Auth  ·  Leaderboard  ·  Score Submission  │
 │                                              │
 │        Service Layer  ·  JWT Security        │
 │                                              │
 │   Kafka Producer  ──────►  Kafka Consumer    │
 │                                  │           │
 │                            Redis Repository  │
 │                                  │           │
 │              LeaderboardStreamBroadcaster    │
 │              (fans out to gRPC + WebSocket)  │
 └──────────────────────────────────────────────┘
        │                               │
   Apache Kafka                      Redis 7
   (6 partitions)               Sorted Sets + Profiles
```

### Score Submission Flow

```
POST /api/scores  ──►  Kafka (keyed by userId)  ──►  Consumer (3 threads)
                                                          │
                                                    Redis ZADD
                                                          │
                                             gRPC Streams + WebSocket push
```

---

## Tech Stack

| Concern | Technology |
|---|---|
| Language & Runtime | Java 21 |
| Web Framework | Spring Boot 3.2.5 |
| RPC | gRPC 1.61 + Protocol Buffers 3.25 |
| Message Bus | Apache Kafka (Confluent 7.6) |
| Storage | Redis 7.2 — Sorted Sets + JSON profiles |
| Redis Client | Lettuce with connection pool |
| Security | Spring Security + JJWT 0.12.5 |
| Real-time | WebSocket / STOMP |
| Observability | Spring Actuator + Micrometer + Prometheus |
| Build | Maven 3.9 · Multi-stage Docker |

---

## Key Engineering Decisions

**Redis Sorted Sets for ranking**
`ZADD` and `ZREVRANGE` give O(log N) writes and O(log N + k) reads. Separate sorted sets per game (`leaderboard:game:{gameId}`) allow independent game leaderboards without any cross-game interference.

**Kafka as the score ingestion bus**
The HTTP/gRPC write path publishes to Kafka and returns immediately. The consumer updates Redis asynchronously. This means: the API stays fast under load, scores are durable even if Redis hiccups, and the consumer tier can scale independently of the API tier.

**Per-user Kafka partition key**
All score events for a given user are routed to the same partition (key = `userId`). This preserves ordering per user while allowing full parallelism across users.

**Manual Kafka ACK + Dead Letter Topic**
Offsets are committed only after a successful Redis write. Failed records are retried 3× and then forwarded to `leaderboard-score-events.DLT` — nothing is silently dropped.

**Dual streaming transports**
gRPC server-streaming (`StreamLeaderboard`) serves native/backend clients. WebSocket/STOMP serves browser clients. Both are driven by the same `LeaderboardStreamBroadcaster`, using `ConcurrentHashMap + CopyOnWriteArrayList` for lock-free fan-out.

**Username-keyed user profiles**
Profiles live at `user:profile:{username}` — login is a single O(1) Redis `GET` with no secondary index. The internal `userId` UUID (stored inside the profile JSON) is used in JWTs and sorted sets so users can rename without losing score history.

**Non-root Docker container**
Multi-stage build (Maven JDK 21 → JRE 21). The application runs as an unprivileged `scoreboard` user with G1GC and container-aware JVM memory flags.

---

## Project Structure

```
src/main/java/com/scoreboard/
├── config/          AsyncConfig · KafkaConfig · RedisConfig
│                    SecurityConfig · WebSocketConfig · CorsConfig
├── controller/      AuthController · LeaderboardController · GlobalExceptionHandler
├── grpc/            LeaderboardGrpcService   (all 7 RPCs)
├── kafka/           ScoreEventProducer · ScoreEventConsumer (+ DLT handler)
├── model/           ScoreEvent · LeaderboardEntry · UserProfile
├── repository/      RedisLeaderboardRepository (interface + Lettuce impl)
├── security/        JwtUtil · JwtAuthenticationFilter · JwtServerInterceptor
├── service/         AuthService · LeaderboardService · ScoreService
│                    LeaderboardStreamBroadcaster
└── scheduler/       PeriodicLeaderboardScheduler

src/main/proto/
└── leaderboard.proto   (service contract — 7 RPCs, 12 message types)

src/main/resources/static/
└── index.html · game.html   (built-in browser demo UI)
```

---

## API Surface

### REST  `port 8080`

All endpoints (except `/api/auth/**`) require `Authorization: Bearer <token>`.

| Method | Endpoint | Purpose |
|---|---|---|
| `POST` | `/api/auth/register` | Create account |
| `POST` | `/api/auth/login` | Get JWT token |
| `POST` | `/api/scores` | Submit a score (async via Kafka) |
| `GET` | `/api/leaderboard/global` | Paginated global board |
| `GET` | `/api/leaderboard/game/{gameId}` | Per-game board |
| `GET` | `/api/leaderboard/rank/{userId}` | User's current rank |

### gRPC  `port 9090`

```protobuf
service LeaderboardService {
  rpc Register(RegisterRequest)           returns (RegisterResponse);
  rpc Login(LoginRequest)                 returns (LoginResponse);
  rpc SubmitScore(SubmitScoreRequest)     returns (SubmitScoreResponse);
  rpc GetGlobalLeaderboard(...)           returns (LeaderboardResponse);
  rpc GetGameLeaderboard(...)             returns (LeaderboardResponse);
  rpc GetUserRank(GetUserRankRequest)     returns (GetUserRankResponse);
  rpc StreamLeaderboard(StreamRequest)   returns (stream LeaderboardUpdate);
}
```

### WebSocket  `ws://localhost:8080/ws`

Clients subscribe to `/topic/leaderboard/{gameId}` (or `/topic/leaderboard/global`) and receive `LeaderboardUpdate` messages pushed after every score event.

---

## Running Locally

### Docker Compose — full stack in one command

```bash
docker-compose up -d
```

| Service | URL |
|---|---|
| REST API + Demo UI | http://localhost:8080 |
| gRPC | localhost:9090 |
| Demo game | http://localhost:8080/game.html |
| Health check | http://localhost:8080/actuator/health |
| Kafka UI | http://localhost:8090 |
| Redis Commander | http://localhost:8091 |

### Without Docker

```bash
  mvn clean package -DskipTests
  java -jar target/RealTimeScoreboard-1.0.0.jar
```

Requires Redis on `localhost:6379` and Kafka on `localhost:9092`, or override via `--spring.data.redis.host=...` and `--spring.kafka.bootstrap-servers=...`.

---

## Tests

```bash
  mvn test
```

| Test class | What it covers |
|---|---|
| `AuthServiceTest` | Registration, duplicate usernames, login, JWT issuance |
| `LeaderboardServiceTest` | Score writes, rank queries, pagination |
| `ScoreboardIntegrationTest` | End-to-end: Kafka → Redis → leaderboard read |

Test profile uses embedded Kafka and a local Redis instance (`application-test.yml`).

---

## Environment Variables

| Variable | Default | Notes |
|---|---|---|
| `JWT_SECRET` | *(insecure placeholder)* | **Change in production** — min 32 chars |
| `KAFKA_BROKERS` | `localhost:9092` | Kafka bootstrap address |
| `REDIS_HOST` | `localhost` | Redis hostname |
| `REDIS_PORT` | `6379` | Redis port |
| `REDIS_PASSWORD` | *(empty)* | Set if Redis AUTH is enabled |

---

## Observability

Spring Actuator exposes `/actuator/health`, `/actuator/metrics`, and `/actuator/prometheus` out of the box. Kafka consumer metadata (partition, offset) and Redis operations are logged at `DEBUG` level for `com.scoreboard.*`.