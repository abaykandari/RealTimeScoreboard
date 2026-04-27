package com.scoreboard.service;

import com.scoreboard.kafka.ScoreEventProducer;
import com.scoreboard.model.ScoreEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * Orchestrates a score submission:
 *   validate → build ScoreEvent → publish to Kafka (async)
 *
 * The Kafka consumer will later call LeaderboardService.updateScore()
 * which persists to Redis and triggers WebSocket/gRPC broadcast.
 *
 * This service returns the user's PRE-UPDATE rank immediately so the
 * caller gets fast feedback. The stream will push the authoritative
 * post-update rank once the consumer completes.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ScoreService {

    private final ScoreEventProducer  producer;
    private final LeaderboardService  leaderboardService;

    public long submitScore(String userId, String username, String gameId, double score) {
        if (score < 0) {
            throw new IllegalArgumentException("Score must be non-negative, got: " + score);
        }
        if (gameId == null || gameId.isBlank()) {
            throw new IllegalArgumentException("gameId must not be blank");
        }

        ScoreEvent event = ScoreEvent.builder()
                .userId(userId)
                .username(username)
                .gameId(gameId)
                .score(score)
                .eventType(ScoreEvent.EventType.SCORE_UPDATE)
                .build();

        producer.publishScoreEvent(event);   // non-blocking; Kafka ack via callback

        // Return stale-but-fast current rank for immediate UI feedback
        long currentRank = leaderboardService.getUserRank(userId, null).getRank();
        log.info("[ScoreService] Queued score userId={} game={} score={} currentRank={}",
                userId, gameId, score, currentRank);
        return currentRank;
    }
}
