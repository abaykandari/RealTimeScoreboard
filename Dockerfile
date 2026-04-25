# ─────────────────────────────────────────────────────────────────────────────
#  Multi-stage Dockerfile
#  Stage 1: Build fat JAR with Maven
#  Stage 2: Lean runtime image with JRE 21
# ─────────────────────────────────────────────────────────────────────────────

# ── Stage 1: Build ────────────────────────────────────────────────────────────
FROM maven:3.9.6-eclipse-temurin-21 AS builder

WORKDIR /build

# Cache dependencies — copy only pom first for layer caching
COPY pom.xml .
RUN mvn dependency:go-offline -q

# Copy source and build (skip tests — run them in CI separately)
COPY src ./src
RUN mvn clean package -DskipTests -q

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM eclipse-temurin:21-jre-jammy

LABEL maintainer="scoreboard-team"
LABEL description="Real-Time Scoreboard — Spring Boot + gRPC + Kafka + Redis"

# Security: run as non-root
RUN useradd -ms /bin/bash scoreboard
USER scoreboard

WORKDIR /app

# Copy fat JAR from builder stage
COPY --from=builder /build/target/RealTimeScoreboard-*.jar app.jar

# Expose ports: 8080 (REST/WS), 9090 (gRPC)
EXPOSE 8080 9090

# JVM tuning for containers:
#   UseContainerSupport   — respect cgroup memory limits
#   MaxRAMPercentage      — use up to 75% of container RAM for heap
#   +UseG1GC              — low-pause GC suitable for server workloads
ENTRYPOINT ["java", \
  "-XX:+UseContainerSupport", \
  "-XX:MaxRAMPercentage=75.0", \
  "-XX:+UseG1GC", \
  "-XX:+OptimizeStringConcat", \
  "-Djava.security.egd=file:/dev/./urandom", \
  "-jar", "app.jar"]
