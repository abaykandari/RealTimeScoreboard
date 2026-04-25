package com.scoreboard.config;

import com.scoreboard.model.ScoreEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Kafka infrastructure configuration.
 *
 * Key decisions:
 *  - Producer uses acks=all + idempotence for exactly-once semantics
 *  - Consumer uses MANUAL_IMMEDIATE ack for at-least-once with explicit control
 *  - Error handler retries 3×, then sends to DLT (dead-letter topic)
 *  - Separate consumer factory exposes typed {@code ScoreEvent} objects
 */
@Slf4j
@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${app.kafka.topics.score-events}")
    private String scoreEventsTopic;

    @Value("${app.kafka.topics.score-events-partitions}")
    private int partitions;

    @Value("${app.kafka.topics.score-events-replicas}")
    private int replicas;

    @Value("${spring.kafka.consumer.group-id}")
    private String consumerGroupId;

    // ──────────────────────────────────────────────────────────────────────────
    //  Topic auto-creation
    // ──────────────────────────────────────────────────────────────────────────

    @Bean
    public NewTopic scoreEventsTopic() {
        return TopicBuilder
                .name(scoreEventsTopic)
                .partitions(partitions)
                .replicas(replicas)
                .compact()
                .build();
    }

    /** Dead-letter topic — receives events that failed all retry attempts */
    @Bean
    public NewTopic scoreEventsDlt() {
        return TopicBuilder
                .name(scoreEventsTopic + ".DLT")
                .partitions(1)
                .replicas(replicas)
                .build();
    }

    // ──────────────────────────────────────────────────────────────────────────
    //  Producer
    // ──────────────────────────────────────────────────────────────────────────

    @Bean
    public ProducerFactory<String, ScoreEvent> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        // Batching for throughput
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, ScoreEvent> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    // ──────────────────────────────────────────────────────────────────────────
    //  Consumer
    // ──────────────────────────────────────────────────────────────────────────

    @Bean
    public ConsumerFactory<String, ScoreEvent> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
        // Trust our model package for deserialization
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.scoreboard.model");
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, ScoreEvent.class);
        return new DefaultKafkaConsumerFactory<>(props,
                new StringDeserializer(),
                new JsonDeserializer<>(ScoreEvent.class, false));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, ScoreEvent> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, ScoreEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());

        // Manual acknowledgement — commit only after successful Redis write
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        // 3 concurrent consumer threads (match partition count for max throughput)
        factory.setConcurrency(3);

        // Retry 3× with 1 s backoff; after that, forward to DLT
        factory.setCommonErrorHandler(
                new DefaultErrorHandler(new FixedBackOff(1000L, 3L))
        );

        // Enable batch listener for higher throughput scenarios
        factory.setBatchListener(false);

        return factory;
    }
}
