package com.scoreboard.scheduler;

import com.scoreboard.service.LeaderboardStreamBroadcaster;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Scheduled heartbeat broadcaster.
 *
 * Even when no score events arrive, active stream clients receive
 * a periodic leaderboard snapshot every {@code stream-interval-ms}.
 * This guarantees freshness for long-lived connections.
 *
 * In a clustered deployment this scheduler should run on only one
 * node (e.g. via ShedLock or leader election) to avoid duplicate
 * pushes. For simplicity, concurrent pushes are idempotent here.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PeriodicLeaderboardScheduler {

    private final LeaderboardStreamBroadcaster broadcaster;

    /**
     * Push global leaderboard snapshot every 2 seconds to all subscribers.
     * Fixed-rate chosen over fixed-delay to keep intervals predictable.
     */
    @Scheduled(fixedRateString = "${app.leaderboard.stream-interval-ms}")
    public void broadcastGlobalHeartbeat() {
        try {
            broadcaster.broadcastUpdate("global");
        } catch (Exception e) {
            // Swallow — don't let a failed heartbeat crash the scheduler thread
            log.warn("[Scheduler] Heartbeat broadcast failed: {}", e.getMessage());
        }
    }
}
