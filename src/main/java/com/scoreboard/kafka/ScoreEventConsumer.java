package com.scoreboard.kafka;

import com.scoreboard.model.ScoreEvent;
import com.scoreboard.service.LeaderboardService;
import com.scoreboard.service.LeaderboardStreamBroadcaster;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Kafka consumer — processes score events from the leaderboard topic.
 *
 * Processing pipeline per message:
 *   1. Deserialize {@link ScoreEvent} from Kafka record
 *   2. Write to Redis sorted set via {@link LeaderboardService}
 *   3. Notify gRPC stream observers via {@link LeaderboardStreamBroadcaster}
 *   4. ACK to Kafka (manual, so partial failures don't commit)
 *
 * Concurrency:
 *   Spring Kafka spins up 3 consumer threads (see KafkaConfig).
 *   Each thread is pinned to a subset of partitions by Kafka's
 *   cooperative-sticky rebalancer, ensuring no duplicate processing.
 *
 * Error handling:
 *   Transient errors (Redis timeout, etc.) are retried 3× via the
 *   DefaultErrorHandler configured in KafkaConfig. After exhausting
 *   retries the record is forwarded to the .DLT topic for manual review.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ScoreEventConsumer {

    private final LeaderboardService      leaderboardService;
    private final LeaderboardStreamBroadcaster broadcaster;

    // ──────────────────────────────────────────────────────────────────────────
    //  Main consumer listener
    // ──────────────────────────────────────────────────────────────────────────

    @KafkaListener(
            topics            = "${app.kafka.topics.score-events}",
            groupId           = "${spring.kafka.consumer.group-id}",
            containerFactory  = "kafkaListenerContainerFactory"
    )
    public void onScoreEvent(ConsumerRecord<String, ScoreEvent> record,
                             Acknowledgment ack) {

        ScoreEvent event = record.value();
        log.info("[Consumer] Received ScoreEvent eventId={} userId={} game={} score={} partition={} offset={}",
                event.getEventId(), event.getUserId(), event.getGameId(),
                event.getScore(), record.partition(), record.offset());

        try {
            // Step 1 — update Redis leaderboard (both global and per-game)
            leaderboardService.updateScore(event);

            // Step 2 — push update to all active gRPC / WebSocket subscribers
            broadcaster.broadcastUpdate(event.getGameId());

            // Step 3 — commit offset only after successful processing
            ack.acknowledge();

            log.debug("[Consumer] Successfully processed eventId={}", event.getEventId());

        } catch (Exception e) {
            log.error("[Consumer] Failed to process eventId={} — will retry. Error: {}",
                    event.getEventId(), e.getMessage(), e);
            // Do NOT ack — Spring's DefaultErrorHandler will retry then DLT
            throw e;
        }
    }

    // ──────────────────────────────────────────────────────────────────────────
    //  Dead-letter topic listener (monitor / alerting)
    // ──────────────────────────────────────────────────────────────────────────

    @KafkaListener(
            topics   = "${app.kafka.topics.score-events}.DLT",
            groupId  = "${spring.kafka.consumer.group-id}-dlt"
    )
    public void onDeadLetterEvent(ConsumerRecord<String, ScoreEvent> record,
                                  Acknowledgment ack) {

        ScoreEvent event = record.value();
        log.error("[DLT] Dead-letter event received — manual intervention required. " +
                        "eventId={} userId={} game={} score={} partition={} offset={}",
                event.getEventId(), event.getUserId(), event.getGameId(),
                event.getScore(), record.partition(), record.offset());

        // TODO: wire to PagerDuty / Slack alert in production
        ack.acknowledge();
    }
}
