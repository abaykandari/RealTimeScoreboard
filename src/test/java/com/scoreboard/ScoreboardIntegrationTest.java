/*
package com.scoreboard;

import com.scoreboard.kafka.ScoreEventProducer;
import com.scoreboard.model.ScoreEvent;
import com.scoreboard.service.LeaderboardService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

*/
/**
 * End-to-end integration test.
 *
 * Uses:
 *   - @EmbeddedKafka  — in-process Kafka broker (no Docker required for tests)
 *   - Testcontainers Redis (or mocked via @MockBean for unit tests)
 *
 * Test flow:
 *   Producer.publish → [Kafka] → Consumer.process → Redis.updateScore
 *   Then assert Redis leaderboard reflects the submitted score.
 *//*

@SpringBootTest
@ActiveProfiles("test")
@EmbeddedKafka(
        partitions = 1,
        topics     = {"leaderboard-score-events", "leaderboard-score-events.DLT"}
)
class ScoreboardIntegrationTest {

    @Autowired ScoreEventProducer  producer;
    @Autowired LeaderboardService  leaderboardService;

    @Test
    @DisplayName("Score event published to Kafka should update Redis leaderboard")
    void scoreEventFlowsFromKafkaToRedis() throws Exception {
        // Arrange
        ScoreEvent event = ScoreEvent.builder()
                .userId("user-001")
                .username("alice")
                .gameId("game-chess")
                .score(9500.0)
                .build();

        // Act
        producer.publishScoreEvent(event);

        // Allow Kafka consumer time to process the record
        TimeUnit.SECONDS.sleep(3);

        // Assert — Redis should now have alice's score
        var rank = leaderboardService.getUserRank("user-001", "game-chess");
        assertThat(rank.getScore()).isEqualTo(9500.0);
        assertThat(rank.getUsername()).isEqualTo("alice");
        assertThat(rank.getRank()).isGreaterThan(0);
    }

    @Test
    @DisplayName("Only the highest score per user should be kept in the leaderboard")
    void onlyHighScoreIsRetained() throws Exception {
        // Arrange — submit two scores for same user
        String userId = "user-002";
        ScoreEvent low = ScoreEvent.builder()
                .userId(userId).username("bob").gameId("game-chess").score(1000.0).build();
        ScoreEvent high = ScoreEvent.builder()
                .userId(userId).username("bob").gameId("game-chess").score(5000.0).build();
        ScoreEvent lower = ScoreEvent.builder()
                .userId(userId).username("bob").gameId("game-chess").score(3000.0).build();

        producer.publishScoreEvent(low);
        producer.publishScoreEvent(high);
        producer.publishScoreEvent(lower);

        TimeUnit.SECONDS.sleep(3);

        // Assert — only the high score (5000) should be stored
        var rank = leaderboardService.getUserRank(userId, "game-chess");
        assertThat(rank.getScore()).isEqualTo(5000.0);
    }
}
*/
