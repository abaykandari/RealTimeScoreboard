package com.scoreboard.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;

/**
 * ScoreEvent is the canonical domain object that flows through the pipeline:
 *
 *   gRPC handler → KafkaProducer → [Kafka topic] → KafkaConsumer → Redis
 *
 * It is also pushed to WebSocket subscribers after Redis update.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ScoreEvent implements Serializable {

    /** Unique event ID for idempotency checks */
    @Builder.Default
    private String eventId = UUID.randomUUID().toString();

    @JsonProperty("user_id")
    private String userId;

    private String username;

    @JsonProperty("game_id")
    private String gameId;

    private double score;

    /** Wall-clock time the score was submitted */
    @Builder.Default
    private long timestamp = Instant.now().toEpochMilli();

    /** e.g. SCORE_UPDATE, USER_REGISTERED */
    @Builder.Default
    private EventType eventType = EventType.SCORE_UPDATE;

    public enum EventType {
        SCORE_UPDATE,
        USER_REGISTERED,
        RANK_CHANGE
    }
}
