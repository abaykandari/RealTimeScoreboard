package com.scoreboard.kafka;

import com.scoreboard.model.ScoreEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

/**
 * Kafka producer — publishes {@link ScoreEvent} records to the score-events topic.
 *
 * Partitioning strategy:
 *   Key = userId  →  all events for the same user land on the same partition.
 *   This preserves per-user ordering while allowing parallel processing across users.
 *
 * The send is fully async. The caller gets back a {@link CompletableFuture} so it can
 * choose to block, chain callbacks, or fire-and-forget.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ScoreEventProducer {

    private final KafkaTemplate<String, ScoreEvent> kafkaTemplate;

    @Value("${app.kafka.topics.score-events}")
    private String scoreEventsTopic;

    // ──────────────────────────────────────────────────────────────────────────
    //  Primary publish method
    // ──────────────────────────────────────────────────────────────────────────

    /**
     * Publish a score event asynchronously.
     *
     * @param event the domain event to publish
     * @return future that completes with metadata on success or with exception on failure
     */
    public CompletableFuture<SendResult<String, ScoreEvent>> publishScoreEvent(ScoreEvent event) {
        // Use userId as partition key for ordering guarantees per user
        ProducerRecord<String, ScoreEvent> record =
                new ProducerRecord<>(scoreEventsTopic, event.getUserId(), event);

        CompletableFuture<SendResult<String, ScoreEvent>> future =
                kafkaTemplate.send(record);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("[Kafka] Failed to publish ScoreEvent eventId={} userId={} error={}",
                        event.getEventId(), event.getUserId(), ex.getMessage(), ex);
            } else {
                RecordMetadata meta = result.getRecordMetadata();
                log.debug("[Kafka] Published ScoreEvent eventId={} userId={} topic={} partition={} offset={}",
                        event.getEventId(),
                        event.getUserId(),
                        meta.topic(),
                        meta.partition(),
                        meta.offset());
            }
        });

        return future;
    }

    /**
     * Convenience method: block until Kafka broker acknowledges the record.
     * Use this only in tests or flows where you must confirm delivery before proceeding.
     */
    public RecordMetadata publishScoreEventSync(ScoreEvent event) {
        try {
            SendResult<String, ScoreEvent> result = publishScoreEvent(event).get();
            return result.getRecordMetadata();
        } catch (Exception e) {
            log.error("[Kafka] Synchronous publish failed for eventId={}", event.getEventId(), e);
            throw new RuntimeException("Kafka publish failed", e);
        }
    }
}
