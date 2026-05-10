package com.scoreboard.repository;

import com.scoreboard.model.LeaderboardEntry;
import com.scoreboard.model.UserProfile;

import java.util.List;
import java.util.Optional;

/**
 * Repository interface for all leaderboard persistence operations.
 *
 * PROFILE KEY DESIGN (v2):
 * ─────────────────────────
 * Profiles are now stored at:  user:profile:{username}
 *
 * NEW DESIGN:
 *   - Key  = user:profile:{username}   (username is the unique identifier)
 *   - No TTL — profiles live until explicitly deleted
 *   - username uniqueness check = check whether the key exists
 *   - login = single GET, no index needed at all
 *   - No username:index HASH needed → removed entirely
 *
 * The userId UUID is still generated on registration and stored INSIDE the
 * profile JSON. It is used in JWTs and leaderboard sorted sets so that a
 * user can rename their username later without breaking their score history
 * (the ZSET member stays as userId, only the display name changes).
 */
public interface RedisLeaderboardRepository {

    // ── Score writes ─────────────────────────────────────────────────────────

    void updateScore(String userId, String username, String gameId, double score);

    // ── Leaderboard reads ────────────────────────────────────────────────────

    List<LeaderboardEntry> getGlobalLeaderboard(int limit, int offset);

    List<LeaderboardEntry> getGameLeaderboard(String gameId, int limit, int offset);

    /** @param gameId null or blank → global board */
    LeaderboardEntry getUserRank(String userId, String gameId);

    long getGlobalLeaderboardSize();

    // ── User profile ─────────────────────────────────────────────────────────

    /**
     * Save a user profile.
     * Key: user:profile:{username}
     * No TTL — profiles persist until explicitly deleted.
     */
    void saveUserProfile(UserProfile profile);

    /**
     * Look up a profile by username.
     * Single O(1) GET — no secondary index involved.
     *
     * @param username the plain username string (not userId)
     */
    Optional<UserProfile> findProfileByUsername(String username);

    /**
     * Check if a username is already registered.
     * Implemented as EXISTS user:profile:{username} — O(1).
     */
    boolean usernameExists(String username);

    /** No-op default — index removed. Exists only so old test verify(never()) compiles. */
    default void saveUsernameIndex(String username, String userId) {}
}
