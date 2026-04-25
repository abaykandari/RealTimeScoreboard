package com.scoreboard.service;

import com.scoreboard.model.LeaderboardEntry;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Central broadcaster that fans out leaderboard updates to:
 *   1. All active gRPC streaming observers for a given gameId
 *   2. WebSocket /topic/leaderboard/{gameId} (STOMP)
 *
 * The observer registry uses ConcurrentHashMap + CopyOnWriteArrayList
 * for lock-free reads (the common case) and safe concurrent writes.
 *
 * Called by the Kafka consumer after each successful Redis update.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class LeaderboardStreamBroadcaster {

    private final LeaderboardService    leaderboardService;
    private final SimpMessagingTemplate messagingTemplate;

    @Value("${app.leaderboard.default-top-n}")
    private int defaultTopN;

    /**
     * Registry: gameId → list of active gRPC stream observers.
     * "global" is used as the gameId key for the global board.
     */
    private final ConcurrentHashMap<String, CopyOnWriteArrayList<ObserverEntry>>
            observerRegistry = new ConcurrentHashMap<>();

    // ──────────────────────────────────────────────────────────────────────────
    //  gRPC observer management
    // ──────────────────────────────────────────────────────────────────────────

    /**
     * Register a new gRPC stream observer for a given gameId.
     * The observer will receive every future update until it is removed.
     *
     * @param gameId   the game board to watch ("" or "global" for global)
     * @param topN     how many top entries to include in each push
     * @param observer the gRPC StreamObserver to push to
     */
    public void registerObserver(String gameId,
                                 int topN,
                                 StreamObserver<?> observer) {
        String key = normalizeGameId(gameId);
        observerRegistry
                .computeIfAbsent(key, k -> new CopyOnWriteArrayList<>())
                .add(new ObserverEntry(observer, topN));
        log.info("[Broadcaster] Registered gRPC observer for gameId='{}' — total observers: {}",
                key, observerRegistry.get(key).size());
    }

    /**
     * Remove an observer (called on stream cancel / completion / error).
     */
    public void removeObserver(String gameId, StreamObserver<?> observer) {
        String key = normalizeGameId(gameId);
        CopyOnWriteArrayList<ObserverEntry> list = observerRegistry.get(key);
        if (list != null) {
            list.removeIf(e -> e.observer() == observer);
            log.info("[Broadcaster] Removed gRPC observer for gameId='{}' — remaining: {}",
                    key, list.size());
        }
    }

    // ──────────────────────────────────────────────────────────────────────────
    //  Broadcast (called by Kafka consumer)
    // ──────────────────────────────────────────────────────────────────────────

    /**
     * Fetch fresh leaderboard data from Redis and push to all registered consumers.
     *
     * @param gameId the game that just received a score update
     */
    @SuppressWarnings("unchecked")
    public void broadcastUpdate(String gameId) {
        String key = normalizeGameId(gameId);

        // ── gRPC stream push ───────────────────────────────────────────────
        CopyOnWriteArrayList<ObserverEntry> observers = observerRegistry.get(key);
        if (observers != null && !observers.isEmpty()) {
            List<ObserverEntry> stale = new ArrayList<>();

            for (ObserverEntry entry : observers) {
                try {
                    List<LeaderboardEntry> entries = fetchEntries(gameId, entry.topN());
                    // The raw observer is typed by the gRPC service; we cast via the
                    // LeaderboardGrpcService which owns the typed observer.
                    // We signal via a lightweight callback interface instead.
                    entry.push(entries, gameId);
                } catch (Exception e) {
                    log.warn("[Broadcaster] Dead gRPC observer detected for gameId='{}': {}",
                            key, e.getMessage());
                    stale.add(entry);
                }
            }
            observers.removeAll(stale);
        }

        // ── WebSocket STOMP push ───────────────────────────────────────────
        List<LeaderboardEntry> wsEntries = fetchEntries(gameId, defaultTopN);
        WebSocketLeaderboardPayload payload = new WebSocketLeaderboardPayload(
                gameId,
                wsEntries,
                Instant.now().toEpochMilli()
        );

        String destination = key.equals("global")
                ? "/topic/leaderboard/global"
                : "/topic/leaderboard/" + gameId;

        messagingTemplate.convertAndSend(destination, payload);
        log.debug("[Broadcaster] WebSocket push to {} — {} entries", destination, wsEntries.size());
    }

    // ──────────────────────────────────────────────────────────────────────────
    //  Helpers
    // ──────────────────────────────────────────────────────────────────────────

    private List<LeaderboardEntry> fetchEntries(String gameId, int topN) {
        if (gameId == null || gameId.isBlank() || gameId.equals("global")) {
            return leaderboardService.getGlobalLeaderboard(topN, 0);
        }
        return leaderboardService.getGameLeaderboard(gameId, topN, 0);
    }

    private String normalizeGameId(String gameId) {
        return (gameId == null || gameId.isBlank()) ? "global" : gameId;
    }

    // ──────────────────────────────────────────────────────────────────────────
    //  Inner types
    // ──────────────────────────────────────────────────────────────────────────

    /**
     * Wraps a raw gRPC StreamObserver with its topN configuration.
     * The push() method is implemented by the gRPC service layer which
     * builds the typed proto response object.
     */
    public record ObserverEntry(StreamObserver<?> observer, int topN) {
        /** Invoked by broadcaster; gRPC service overrides via pushCallback */
        public void push(List<LeaderboardEntry> entries, String gameId) {
            if (observer instanceof PushCapable pc) {
                pc.push(entries, gameId);
            }
        }
    }

    /** Marker interface for gRPC stream delegates that can receive typed pushes */
    public interface PushCapable {
        void push(List<LeaderboardEntry> entries, String gameId);
    }

    /** WebSocket payload DTO */
    public record WebSocketLeaderboardPayload(
            String gameId,
            List<LeaderboardEntry> entries,
            long timestamp
    ) {}
}
