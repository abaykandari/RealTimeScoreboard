package com.scoreboard.service;

import com.scoreboard.kafka.ScoreEventProducer;
import com.scoreboard.model.ScoreEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * Orchestrates a score submission:
 *   validate → build event → publish to Kafka
 *
 * The actual Redis write happens asynchronously in the Kafka consumer.
 * This keeps the gRPC handler latency low (Kafka ack ~5 ms) regardless
 * of Redis write time.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ScoreService {

    private final ScoreEventProducer   producer;
    private final LeaderboardService   leaderboardService;

    /**
     * Submit a score update for a user.
     *
     * @return the user's current global rank (read from Redis before async write)
     */
    public long submitScore(String userId, String username, String gameId, double score) {
        if (score < 0) {
            throw new IllegalArgumentException("Score must be non-negative");
        }

        ScoreEvent event = ScoreEvent.builder()
                .userId(userId)
                .username(username)
                .gameId(gameId)
                .score(score)
                .eventType(ScoreEvent.EventType.SCORE_UPDATE)
                .build();

        // Publish to Kafka — non-blocking
        producer.publishScoreEvent(event);

        // Return the current (pre-update) global rank for immediate feedback.
        // The rank shown here may be stale by one event; the stream will deliver
        // the authoritative value once the Kafka consumer processes the event.
        long currentRank = leaderboardService.getUserRank(userId, null).getRank();

        log.info("[ScoreService] Score submitted userId={} game={} score={} currentRank={}",
                userId, gameId, score, currentRank);
        return currentRank;
    }
}
