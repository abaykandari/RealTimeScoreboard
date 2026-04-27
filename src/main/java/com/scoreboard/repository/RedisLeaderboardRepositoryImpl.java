package com.scoreboard.repository;

import com.scoreboard.model.LeaderboardEntry;
import com.scoreboard.model.UserProfile;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Repository;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Redis implementation of RedisLeaderboardRepository.
 *
 * Redis key schema:
 *
 *   leaderboard:global              ZSET   member=userId, score=highScore
 *   leaderboard:game:{gameId}       ZSET   member=userId, score=highScore
 *   leaderboard:global:meta         HASH   userId → username
 *   leaderboard:game:{gameId}:meta  HASH   userId → username
 *   user:profile:{userId}           JSON   UserProfile object
 *   username:index                  HASH   username → userId  ← secondary index for login
 */
@Slf4j
@Repository
@RequiredArgsConstructor
public class RedisLeaderboardRepositoryImpl implements RedisLeaderboardRepository {

    private final RedisTemplate<String, String>      stringRedisTemplate;
    private final RedisTemplate<String, UserProfile> userProfileTemplate;

    @Value("${app.redis.keys.global-leaderboard}")
    private String globalLeaderboardKey;

    @Value("${app.redis.keys.game-leaderboard-prefix}")
    private String gameLeaderboardPrefix;

    @Value("${app.redis.keys.user-profile-prefix}")
    private String userProfilePrefix;

    @Value("${app.redis.ttl.user-profile-seconds}")
    private long userProfileTtlSeconds;

    // Secondary index key (HASH: username → userId)
    private static final String USERNAME_INDEX = "username:index";

    // ── Lua: atomic ZADD GT (update only if new score is higher) ─────────────
    private static final String ZADD_GT_LUA = """
        local cur = redis.call('ZSCORE', KEYS[1], ARGV[1])
        if cur == false or tonumber(ARGV[2]) > tonumber(cur) then
            redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
            return 1
        end
        return 0
        """;

    // ── Score writes ─────────────────────────────────────────────────────────

    @Override
    public void updateScore(String userId, String username, String gameId, double score) {
        String s = String.valueOf(score);

        // Global leaderboard ZSET
        zaddGt(globalLeaderboardKey, userId, s);
        stringRedisTemplate.opsForHash().put(globalLeaderboardKey + ":meta", userId, username);

        // Per-game leaderboard ZSET
        String gameKey = gameLeaderboardPrefix + gameId;
        zaddGt(gameKey, userId, s);
        stringRedisTemplate.opsForHash().put(gameKey + ":meta", userId, username);

        log.debug("[Redis] updateScore userId={} game={} score={}", userId, gameId, score);
    }

    private void zaddGt(String key, String member, String score) {
        stringRedisTemplate.execute(
            conn -> conn.eval(
                ZADD_GT_LUA.getBytes(),
                ReturnType.INTEGER,
                1,
                key.getBytes(),
                member.getBytes(),
                score.getBytes()
            ),
            true
        );
    }

    // ── Leaderboard reads ────────────────────────────────────────────────────

    @Override
    public List<LeaderboardEntry> getGlobalLeaderboard(int limit, int offset) {
        return fetchLeaderboard(globalLeaderboardKey, limit, offset);
    }

    @Override
    public List<LeaderboardEntry> getGameLeaderboard(String gameId, int limit, int offset) {
        return fetchLeaderboard(gameLeaderboardPrefix + gameId, limit, offset);
    }

    private List<LeaderboardEntry> fetchLeaderboard(String key, int limit, int offset) {
        Set<ZSetOperations.TypedTuple<String>> tuples =
            stringRedisTemplate.opsForZSet()
                .reverseRangeWithScores(key, offset, (long) offset + limit - 1);

        if (tuples == null || tuples.isEmpty()) return List.of();

        List<LeaderboardEntry> entries = new ArrayList<>(tuples.size());
        long rank = offset + 1;
        for (ZSetOperations.TypedTuple<String> t : tuples) {
            String uid = t.getValue();
            double sc  = t.getScore() != null ? t.getScore() : 0.0;
            String uname = (String) stringRedisTemplate.opsForHash()
                .get(key + ":meta", uid);
            entries.add(LeaderboardEntry.builder()
                .rank(rank++).userId(uid != null ? uid : "")
                .username(uname != null ? uname : uid)
                .score(sc).build());
        }
        return entries;
    }

    @Override
    public LeaderboardEntry getUserRank(String userId, String gameId) {
        String key = (gameId == null || gameId.isBlank())
            ? globalLeaderboardKey
            : gameLeaderboardPrefix + gameId;

        Long   rank  = stringRedisTemplate.opsForZSet().reverseRank(key, userId);
        Double score = stringRedisTemplate.opsForZSet().score(key, userId);
        String uname = (String) stringRedisTemplate.opsForHash().get(key + ":meta", userId);

        return LeaderboardEntry.builder()
            .rank(rank != null ? rank + 1 : -1L)
            .userId(userId)
            .username(uname != null ? uname : userId)
            .score(score != null ? score : 0.0)
            .build();
    }

    @Override
    public long getGlobalLeaderboardSize() {
        Long sz = stringRedisTemplate.opsForZSet().size(globalLeaderboardKey);
        return sz != null ? sz : 0L;
    }

    // ── User profile ─────────────────────────────────────────────────────────

    @Override
    public void saveUserProfile(UserProfile profile) {
        userProfileTemplate.opsForValue().set(
            userProfilePrefix + profile.getUserId(),
            profile,
            userProfileTtlSeconds,
            TimeUnit.SECONDS
        );
    }

    @Override
    public Optional<UserProfile> findUserProfile(String userId) {
        UserProfile p = userProfileTemplate.opsForValue()
            .get(userProfilePrefix + userId);
        return Optional.ofNullable(p);
    }

    @Override
    public boolean usernameExists(String username) {
        // Check the secondary index HASH — O(1)
        return Boolean.TRUE.equals(
            stringRedisTemplate.opsForHash().hasKey(USERNAME_INDEX, username)
        );
    }

    // ── Secondary index (THE FIX for login) ──────────────────────────────────

    /**
     * HSET username:index <username> <userId>
     *
     * Stores a permanent mapping so login can look up a profile by username.
     * No TTL — this index lives as long as the user exists.
     */
    @Override
    public void saveUsernameIndex(String username, String userId) {
        stringRedisTemplate.opsForHash().put(USERNAME_INDEX, username, userId);
        log.debug("[Redis] Saved username index: {} → {}", username, userId);
    }

    /**
     * HGET username:index <username>  → userId
     *
     * Step 1 of login. If this returns null, the username was never registered.
     */
    @Override
    public String findUserIdByUsername(String username) {
        return (String) stringRedisTemplate.opsForHash().get(USERNAME_INDEX, username);
    }
}
