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
 * Core leaderboard service — pure business logic, zero Redis coupling.
 * All storage is delegated to RedisLeaderboardRepository.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class LeaderboardService {

    private final RedisLeaderboardRepository repository;

    // ── Write path (called by Kafka consumer) ─────────────────────────────────

    public void updateScore(ScoreEvent event) {
        repository.updateScore(
                event.getUserId(),
                event.getUsername(),
                event.getGameId(),
                event.getScore()
        );
    }

    // ── Leaderboard reads ─────────────────────────────────────────────────────

    public List<LeaderboardEntry> getGlobalLeaderboard(int limit, int offset) {
        return repository.getGlobalLeaderboard(limit, offset);
    }

    public List<LeaderboardEntry> getGameLeaderboard(String gameId, int limit, int offset) {
        return repository.getGameLeaderboard(gameId, limit, offset);
    }

    public LeaderboardEntry getUserRank(String userId, String gameId) {
        return repository.getUserRank(userId, gameId);
    }

    public long getGlobalLeaderboardSize() {
        return repository.getGlobalLeaderboardSize();
    }

    // ── User profile ──────────────────────────────────────────────────────────

    public void saveUserProfile(UserProfile profile) {
        repository.saveUserProfile(profile);
    }

    /** Look up by username — the only access pattern we need */
    public Optional<UserProfile> findProfileByUsername(String username) {
        return repository.findProfileByUsername(username);
    }

    public boolean usernameExists(String username) {
        return repository.usernameExists(username);
    }
}
