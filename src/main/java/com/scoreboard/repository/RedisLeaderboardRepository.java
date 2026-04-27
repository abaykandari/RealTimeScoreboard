package com.scoreboard.repository;

import com.scoreboard.model.LeaderboardEntry;
import com.scoreboard.model.UserProfile;

import java.util.List;
import java.util.Optional;

/**
 * Repository interface for all leaderboard persistence operations.
 *
 * WHY THIS EXISTS:
 * RedisTemplate is a concrete class — extremely hard to mock with Mockito on
 * Java 21+ due to Byte Buddy JVM restrictions. Hiding Redis behind this plain
 * interface means unit tests use a simple JDK proxy mock, zero instrumentation.
 */
public interface RedisLeaderboardRepository {

    // ── Score writes ─────────────────────────────────────────────────────────

    /**
     * Update score in global + game-specific leaderboard.
     * GT semantics: only writes if new score > existing score (personal best).
     */
    void updateScore(String userId, String username, String gameId, double score);

    // ── Leaderboard reads ────────────────────────────────────────────────────

    List<LeaderboardEntry> getGlobalLeaderboard(int limit, int offset);

    List<LeaderboardEntry> getGameLeaderboard(String gameId, int limit, int offset);

    /** @param gameId null or blank → global board */
    LeaderboardEntry getUserRank(String userId, String gameId);

    long getGlobalLeaderboardSize();

    // ── User profile ─────────────────────────────────────────────────────────

    void saveUserProfile(UserProfile profile);

    Optional<UserProfile> findUserProfile(String userId);

    boolean usernameExists(String username);

    /**
     * Write the secondary index:  HSET username:index <username> <userId>
     *
     * THE FIX for the broken login bug.
     * Without this, login has no way to go from a username string to a userId,
     * which is what's needed to look up the profile in Redis.
     *
     * Call this immediately after saveUserProfile() on every registration.
     */
    void saveUsernameIndex(String username, String userId);

    /**
     * Reverse-lookup: username → userId via secondary index.
     * HGET username:index <username>
     *
     * @return userId, or null if username was never registered
     */
    String findUserIdByUsername(String username);
}
