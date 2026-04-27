package com.scoreboard.service;

import com.scoreboard.model.LeaderboardEntry;
import com.scoreboard.model.ScoreEvent;
import com.scoreboard.model.UserProfile;
import com.scoreboard.repository.RedisLeaderboardRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

/**
 * Core leaderboard service.
 *
 * This class contains ONLY business logic — all Redis operations are
 * delegated to {@link RedisLeaderboardRepository}. This makes the
 * service trivially unit-testable: mock the repository interface,
 * no RedisTemplate instrumentation needed.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class LeaderboardService {

    private final RedisLeaderboardRepository repository;

    // ── Write path (called by Kafka consumer) ─────────────────────────────

    /**
     * Process a score event: update both the global and game-specific
     * leaderboards using personal-best semantics (GT semantics in Redis).
     */
    public void updateScore(ScoreEvent event) {
        repository.updateScore(
                event.getUserId(),
                event.getUsername(),
                event.getGameId(),
                event.getScore()
        );
        log.debug("[LeaderboardService] Updated score userId={} game={} score={}",
                event.getUserId(), event.getGameId(), event.getScore());
    }

    // ── Read path ─────────────────────────────────────────────────────────

    public List<LeaderboardEntry> getGlobalLeaderboard(int limit, int offset) {
        return repository.getGlobalLeaderboard(limit, offset);
    }

    public List<LeaderboardEntry> getGameLeaderboard(String gameId, int limit, int offset) {
        return repository.getGameLeaderboard(gameId, limit, offset);
    }

    /**
     * @param gameId pass null or blank for the global leaderboard
     */
    public LeaderboardEntry getUserRank(String userId, String gameId) {
        return repository.getUserRank(userId, gameId);
    }

    public long getGlobalLeaderboardSize() {
        return repository.getGlobalLeaderboardSize();
    }

    // ── User profile ──────────────────────────────────────────────────────

    public void saveUserProfile(UserProfile profile) {
        repository.saveUserProfile(profile);
    }

    public Optional<UserProfile> getUserProfile(String userId) {
        return repository.findUserProfile(userId);
    }

    public boolean userExists(String userId) {
        return repository.findUserProfile(userId).isPresent();
    }

    public boolean usernameExists(String username) {
        return repository.usernameExists(username);
    }
}
