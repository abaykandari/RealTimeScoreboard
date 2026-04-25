package com.scoreboard;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Real-Time Scoreboard — Spring Boot entry point.
 *
 * Components wired up at startup:
 *   • gRPC server  (net.devh grpc-server-spring-boot-starter, port 9090)
 *   • Kafka producer + consumer
 *   • Redis leaderboard via Lettuce connection pool
 *   • WebSocket broadcast hub (bonus)
 */
@SpringBootApplication
@EnableKafka
@EnableAsync
@EnableScheduling
public class ScoreboardApplication {

    public static void main(String[] args) {
        SpringApplication.run(ScoreboardApplication.class, args);
    }
}
