package com.scoreboard.service;

import com.scoreboard.model.LeaderboardEntry;
import com.scoreboard.model.ScoreEvent;
import com.scoreboard.model.UserProfile;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Core leaderboard service backed by Redis Sorted Sets (ZSET).
 *
 * Redis key schema:
 *   leaderboard:global           — global ZSET, member=userId, score=highScore
 *   leaderboard:game:{gameId}    — per-game ZSET
 *   user:profile:{userId}        — JSON hash of UserProfile
 *
 * All ZSET operations are O(log N) on write, O(log N + M) on range read
 * where M is the number of elements returned.  For a leaderboard with
 * millions of users this is well within single-digit millisecond latency.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class LeaderboardService {

    private final RedisTemplate<String, String>      redisTemplate;
    private final RedisTemplate<String, UserProfile> userProfileTemplate;

    @Value("${app.redis.keys.global-leaderboard}")
    private String globalLeaderboardKey;

    @Value("${app.redis.keys.game-leaderboard-prefix}")
    private String gameLeaderboardPrefix;

    @Value("${app.redis.keys.user-profile-prefix}")
    private String userProfilePrefix;

    @Value("${app.redis.ttl.user-profile-seconds}")
    private long userProfileTtlSeconds;

    // ──────────────────────────────────────────────────────────────────────────
    //  Write path  (called by Kafka consumer)
    // ──────────────────────────────────────────────────────────────────────────

    /**
     * Update both the global and game-specific leaderboards with this score.
     *
     * Uses ZADD with the NX / GT semantics:
     *   - Only updates if the new score is greater than the stored score.
     *   - Stores the userId as the sorted-set member, score as ZSET score.
     *
     * Note: Spring's RedisTemplate wraps ZADD; for GT semantics we execute
     * a raw Lua script to remain atomic.
     */
    public void updateScore(ScoreEvent event) {
        String userId  = event.getUserId();
        double score   = event.getScore();
        String gameKey = gameLeaderboardPrefix + event.getGameId();

        // ── Global leaderboard ─────────────────────────────────────────────
        // ZADD leaderboard:global GT userId score
        updateZSetIfHigher(globalLeaderboardKey, userId, score);

        // ── Game-specific leaderboard ──────────────────────────────────────
        updateZSetIfHigher(gameKey, userId, score);

        // ── Update username index (score → username mapping) ───────────────
        // Stored as hash: leaderboard:global:meta  HSET userId username
        redisTemplate.opsForHash()
                .put(globalLeaderboardKey + ":meta", userId, event.getUsername());
        redisTemplate.opsForHash()
                .put(gameKey + ":meta", userId, event.getUsername());

        log.debug("[Redis] Updated score for userId={} game={} score={}", userId, event.getGameId(), score);
    }

    /**
     * Atomically update ZSET score only if new score > existing score.
     * Lua script executes atomically on the Redis server.
     */
    private void updateZSetIfHigher(String key, String member, double newScore) {
        // Lua: if current score is nil or newScore > current → ZADD
        String lua = """
                local cur = redis.call('ZSCORE', KEYS[1], ARGV[1])
                if cur == false or tonumber(ARGV[2]) > tonumber(cur) then
                    redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
                    return 1
                end
                return 0
                """;
        redisTemplate.execute(
                connection -> connection.eval(
                        lua.getBytes(),
                        org.springframework.data.redis.connection.ReturnType.INTEGER,
                        1,
                        key.getBytes(),
                        member.getBytes(),
                        String.valueOf(newScore).getBytes()
                ),
                true
        );
    }

    // ──────────────────────────────────────────────────────────────────────────
    //  Read path
    // ──────────────────────────────────────────────────────────────────────────

    /**
     * Get global top-N leaderboard entries (descending by score).
     */
    public List<LeaderboardEntry> getGlobalLeaderboard(int limit, int offset) {
        return getLeaderboard(globalLeaderboardKey, limit, offset);
    }

    /**
     * Get per-game leaderboard entries.
     */
    public List<LeaderboardEntry> getGameLeaderboard(String gameId, int limit, int offset) {
        return getLeaderboard(gameLeaderboardPrefix + gameId, limit, offset);
    }

    private List<LeaderboardEntry> getLeaderboard(String key, int limit, int offset) {
        // ZREVRANGE key offset (offset+limit-1) WITHSCORES
        Set<ZSetOperations.TypedTuple<String>> tuples =
                redisTemplate.opsForZSet()
                        .reverseRangeWithScores(key, offset, (long) offset + limit - 1);

        if (tuples == null || tuples.isEmpty()) {
            return List.of();
        }

        List<LeaderboardEntry> entries = new ArrayList<>(tuples.size());
        long rank = offset + 1;

        for (ZSetOperations.TypedTuple<String> tuple : tuples) {
            String userId   = tuple.getValue();
            double score    = tuple.getScore() != null ? tuple.getScore() : 0.0;
            String username = (String) redisTemplate.opsForHash()
                    .get(key + ":meta", userId);

            entries.add(LeaderboardEntry.builder()
                    .rank(rank++)
                    .userId(userId)
                    .username(username != null ? username : userId)
                    .score(score)
                    .build());
        }

        return entries;
    }

    /**
     * Get a specific user's rank and score in a leaderboard.
     *
     * @param gameId empty string or null for global board
     */
    public LeaderboardEntry getUserRank(String userId, String gameId) {
        String key = (gameId == null || gameId.isBlank())
                ? globalLeaderboardKey
                : gameLeaderboardPrefix + gameId;

        Long rank = redisTemplate.opsForZSet().reverseRank(key, userId);
        Double score = redisTemplate.opsForZSet().score(key, userId);
        String username = (String) redisTemplate.opsForHash()
                .get(key + ":meta", userId);

        return LeaderboardEntry.builder()
                .rank(rank != null ? rank + 1 : -1)   // Redis rank is 0-based
                .userId(userId)
                .username(username != null ? username : userId)
                .score(score != null ? score : 0.0)
                .build();
    }

    /** Total number of players in the global board */
    public long getGlobalLeaderboardSize() {
        Long size = redisTemplate.opsForZSet().size(globalLeaderboardKey);
        return size != null ? size : 0L;
    }

    // ──────────────────────────────────────────────────────────────────────────
    //  User profile storage
    // ──────────────────────────────────────────────────────────────────────────

    public void saveUserProfile(UserProfile profile) {
        String key = userProfilePrefix + profile.getUserId();
        userProfileTemplate.opsForValue().set(key, profile, userProfileTtlSeconds, TimeUnit.SECONDS);
        log.debug("[Redis] Saved user profile for userId={}", profile.getUserId());
    }

    public UserProfile getUserProfile(String userId) {
        return userProfileTemplate.opsForValue().get(userProfilePrefix + userId);
    }

    public boolean userExists(String userId) {
        return Boolean.TRUE.equals(userProfileTemplate.hasKey(userProfilePrefix + userId));
    }

    /** Check by username — linear scan of stored profiles (for registration only) */
    public boolean usernameExists(String username) {
        Set<String> keys = redisTemplate.keys(userProfilePrefix + "*");
        if (keys == null) return false;
        for (String key : keys) {
            UserProfile p = userProfileTemplate.opsForValue().get(key);
            if (p != null && username.equals(p.getUsername())) return true;
        }
        return false;
    }
}
