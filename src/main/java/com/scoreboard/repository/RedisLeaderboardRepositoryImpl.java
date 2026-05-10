package com.scoreboard.repository;

import com.scoreboard.model.LeaderboardEntry;
import com.scoreboard.model.UserProfile;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Repository;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

/**
 * ════════════════════════════════════════════════════════════════════════════
 *  RedisLeaderboardRepositoryImpl
 *  — the only class in the project that touches RedisTemplate directly.
 * ════════════════════════════════════════════════════════════════════════════
 *
 * DESIGN PRINCIPLE — WHY THIS CLASS EXISTS AS A SEPARATE REPOSITORY:
 * ─────────────────────────────────────────────────────────────────────
 * RedisTemplate is a concrete Spring class. Mockito cannot mock concrete
 * classes cleanly on Java 21+ due to Byte Buddy JVM instrumentation
 * restrictions. Hiding all Redis calls behind the RedisLeaderboardRepository
 * interface means:
 *   • Unit tests mock the interface via a plain JDK proxy — zero Byte Buddy.
 *   • LeaderboardService and AuthService contain pure business logic only.
 *   • Swapping Redis for another store (Memcached, DynamoDB) touches only this file.
 *
 * REDIS KEY SCHEMA:
 * ─────────────────────────────────────────────────────────────────────
 *  user:profile:{username}              STRING  JSON-serialised UserProfile
 *  leaderboard:global                   ZSET    member=userId, score=highScore
 *  leaderboard:game:{gameId}            ZSET    member=userId, score=highScore
 *  leaderboard:global:meta              HASH    userId → username  (display names)
 *  leaderboard:game:{gameId}:meta       HASH    userId → username
 *
 * WHY userId IN THE ZSET BUT username AS THE PROFILE KEY:
 * ─────────────────────────────────────────────────────────────────────
 * The ZSET member is userId (a UUID) so that if a player ever renames
 * themselves, their score history stays intact — only the profile key
 * and the :meta hash entry need updating, not every ZSET across every game.
 * The profile key uses username because login looks up by username directly
 * (GET user:profile:{username}) — no secondary index needed.
 *
 * RTT = Redis Round Trip Time (one network request to Redis and back).
 * Reducing RTTs is the primary performance goal of this class.
 */
@Slf4j
@Repository
@RequiredArgsConstructor
public class RedisLeaderboardRepositoryImpl implements RedisLeaderboardRepository {

    /*
     * Two separate RedisTemplate beans are injected here.
     *
     * stringRedisTemplate  — RedisTemplate<String, String>
     *   Serialises both keys and values as plain UTF-8 strings.
     *   Used for ZSET operations (members are userIds, scores are doubles)
     *   and HASH operations (userId → username mappings).
     *   Choosing String serialisation means the data is human-readable in
     *   redis-cli — you can run ZRANGE leaderboard:global 0 -1 WITHSCORES
     *   and see actual userId strings, not binary blobs.
     *
     * userProfileTemplate  — RedisTemplate<String, UserProfile>
     *   Serialises values as JSON via Jackson2JsonRedisSerializer.
     *   Keys are still plain strings. Used only for profile storage.
     *   Keeping it separate from stringRedisTemplate avoids type confusion —
     *   if you accidentally call userProfileTemplate on a ZSET key you get
     *   a clear type error rather than garbled binary data.
     */
    private final RedisTemplate<String, String>      stringRedisTemplate;
    private final RedisTemplate<String, UserProfile> userProfileTemplate;

    @Value("${app.redis.keys.global-leaderboard}")
    private String globalLeaderboardKey;        // "leaderboard:global"

    @Value("${app.redis.keys.game-leaderboard-prefix}")
    private String gameLeaderboardPrefix;       // "leaderboard:game:"

    @Value("${app.redis.keys.user-profile-prefix}")
    private String userProfilePrefix;           // "user:profile:"


    // ════════════════════════════════════════════════════════════════════════
    //  SCORE WRITES
    // ════════════════════════════════════════════════════════════════════════

    /**
     * Called by the Kafka consumer after reading a ScoreEvent from the topic.
     *
     * WHAT NEEDS TO HAPPEN ON EVERY SCORE SUBMISSION:
     * ─────────────────────────────────────────────────
     *  1. Update the global leaderboard ZSET  (if new score > stored score)
     *  2. Update the game-specific ZSET       (same condition)
     *  3. Store userId → username in the global :meta HASH
     *  4. Store userId → username in the game   :meta HASH
     *
     * Operations 1 and 2 are conditional (GT semantics — only write if higher).
     * Operations 3 and 4 are unconditional overwrites.
     *
     * EXECUTION STRATEGY — TWO PHASES:
     * ─────────────────────────────────
     *  Phase A  [zaddGt calls]:   operations 1 and 2 — sequential, ~2 RTTs
     *  Phase B  [pipeline]:       operations 3 and 4 — batched,    ~1 RTT
     *  Total: ~3 RTTs in the worst case (both ZSETs updated)
     *
     * Why not pipeline all four into one batch?
     *   Operations 1 and 2 are READ-then-WRITE:
     *     READ  current score  (ZSCORE)
     *     WRITE new score only if higher  (ZADD)
     *   A pipeline sends all commands before reading any replies.
     *   You cannot use the reply from ZSCORE to conditionally issue ZADD
     *   in the same pipeline — the pipeline has already been flushed.
     *
     *   Operations 3 and 4 are pure unconditional WRITEs (HSET), so they
     *   CAN safely be pipelined — no read dependency between them.
     */
    @Override
    public void updateScore(String userId, String username, String gameId, double score) {
        String gameKey = gameLeaderboardPrefix + gameId;

        // ── Phase A: conditional ZSET updates (sequential, ~2 RTTs) ──────────
        zaddGt(globalLeaderboardKey, userId, score);
        zaddGt(gameKey,              userId, score);

        // ── Phase B: unconditional metadata writes (pipelined, ~1 RTT) ───────
        stringRedisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            /*
             * executePipelined() puts this connection into "buffer mode".
             * Every command sent through `connection` is NOT executed
             * immediately — it is queued in a client-side buffer.
             * When this lambda returns null, Spring flushes the entire
             * buffer in a single TCP write and reads all replies in one go.
             *
             * We use the low-level connection.hashCommands() API rather than
             * the high-level opsForHash() API because opsForHash() is not
             * available inside a pipeline callback — it would try to obtain
             * a new connection, bypassing the pipeline.
             *
             * bytes() converts String → byte[] because the low-level
             * connection API works with raw bytes, not typed objects.
             */
            connection.hashCommands().hSet(
                    bytes(globalLeaderboardKey + ":meta"),
                    bytes(userId),
                    bytes(username));

            connection.hashCommands().hSet(
                    bytes(gameKey + ":meta"),
                    bytes(userId),
                    bytes(username));

            return null; // required by RedisCallback signature; Spring ignores it
        });

        log.debug("[Redis] updateScore userId={} game={} score={}", userId, gameId, score);
    }

    /**
     * Update a ZSET score only if the new value is strictly greater
     * than what is currently stored — "personal best" semantics.
     *
     * WHY THIS IS SAFE WITHOUT AN ATOMIC OPERATION:
     * ─────────────────────────────────────────────
     * Redis 6.2 introduced the native ZADD GT flag which does this atomically
     * in one command. We deliberately do NOT use it here because:
     *
     *   a) Spring Data Redis 3.2's pipeline API does not expose GT flag
     *      through its high-level typed operations.
     *   b) Accessing the raw Lettuce connection inside a pipeline
     *      (getNativeConnection()) actually BYPASSES the pipeline buffer
     *      and sends commands synchronously — the "optimisation" would
     *      silently become slower than the naive approach.
     *
     * Instead, this two-step ZSCORE → ZADD is safe here because of
     * a guarantee provided by the Kafka configuration:
     *
     *   KafkaProducer keys every ScoreEvent by userId.
     *   Kafka routes all messages with the same key to the same partition.
     *   A Kafka consumer thread owns exactly one partition at a time.
     *   Therefore: all score events for "user-abc123" are always
     *   consumed by the same thread, sequentially, one at a time.
     *
     *   There is no concurrent write for the same userId.
     *   There is no race condition between the ZSCORE and ZADD.
     *   The two-step check is equivalent to the atomic ZADD GT.
     *
     * WHAT EACH REDIS COMMAND DOES:
     * ─────────────────────────────
     *   ZSCORE key member
     *     → returns the score of `member` in the sorted set at `key`.
     *     → returns null if the member does not exist yet.
     *     → O(1) complexity.
     *
     *   ZADD key score member
     *     → adds the member with the given score.
     *     → if the member already exists, updates its score.
     *     → O(log N) complexity (sorted set is a skip list internally).
     */
    private void zaddGt(String key, String member, double score) {
        Double current = stringRedisTemplate.opsForZSet().score(key, member);
        if (current == null || score > current) {
            stringRedisTemplate.opsForZSet().add(key, member, score);
        }
    }


    // ════════════════════════════════════════════════════════════════════════
    //  LEADERBOARD READS
    // ════════════════════════════════════════════════════════════════════════

    @Override
    public List<LeaderboardEntry> getGlobalLeaderboard(int limit, int offset) {
        return fetchLeaderboard(globalLeaderboardKey, limit, offset);
    }

    @Override
    public List<LeaderboardEntry> getGameLeaderboard(String gameId, int limit, int offset) {
        return fetchLeaderboard(gameLeaderboardPrefix + gameId, limit, offset);
    }

    /**
     * Fetch a page of leaderboard entries in descending score order.
     *
     * WHAT EACH REDIS COMMAND DOES:
     * ─────────────────────────────
     *   ZREVRANGE key start stop WITHSCORES
     *     → returns members of the sorted set at `key`, ordered highest
     *       score first (REV = reversed / descending).
     *     → `start` and `stop` are 0-based rank indexes (not scores).
     *       offset=0, limit=10 → positions 0 through 9 → top 10.
     *     → WITHSCORES means each member is paired with its score.
     *     → Returns them in a Set<TypedTuple> where each tuple holds
     *       (member string, score double).
     *     → O(log N + M) where N = total members, M = entries returned.
     *
     *   HMGET key field1 field2 ... fieldN
     *     → returns the values for multiple hash fields in one command.
     *     → Result list is positionally aligned with the input fields list:
     *       result.get(0) is the value for field1, result.get(1) for field2, etc.
     *     → Returns null for any field that does not exist in the hash.
     *     → O(N) where N = number of fields requested.
     *
     * RTT IMPROVEMENT:
     * ─────────────────
     *   OLD approach: 1 ZREVRANGE + N individual HGET calls = N+1 RTTs.
     *   NEW approach: 1 ZREVRANGE + 1 HMGET                = 2 RTTs always.
     *   For a 10-entry leaderboard page: saves 9 network round trips
     *   on every single read request.
     *
     * WHY CONVERT TO ArrayList BEFORE STREAMING:
     * ───────────────────────────────────────────
     *   ZREVRANGE returns a LinkedHashSet (ordered by rank).
     *   LinkedHashSet preserves insertion order, which is the rank order
     *   we need. However, we need positional access (index i) to zip with
     *   the HMGET results. ArrayList gives us O(1) index access.
     *   The zip at the bottom is: ordered.get(i) paired with usernames.get(i).
     *   This is only correct because HMGET preserves the order of the
     *   userIds list we send it — Redis guarantees this.
     */
    private List<LeaderboardEntry> fetchLeaderboard(String key, int limit, int offset) {
        // Command 1: ZREVRANGE key offset (offset+limit-1) WITHSCORES
        Set<ZSetOperations.TypedTuple<String>> tuples =
                stringRedisTemplate.opsForZSet()
                        .reverseRangeWithScores(key, offset, (long) offset + limit - 1);

        if (tuples == null || tuples.isEmpty()) return List.of();

        // Convert to ArrayList to get stable positional index for the zip below.
        // LinkedHashSet preserves rank order; ArrayList lets us do get(i).
        List<ZSetOperations.TypedTuple<String>> ordered = new ArrayList<>(tuples);

        // Extract all userIds in rank order — this becomes the HMGET field list.
        List<Object> userIds = ordered.stream()
                .map(ZSetOperations.TypedTuple::getValue)
                .collect(Collectors.toList());

        // Command 2: HMGET key:meta userId1 userId2 ... userIdN
        // Returns usernames in the SAME ORDER as userIds.
        // result.get(0) = username for userIds.get(0), etc.
        List<Object> usernames = stringRedisTemplate.opsForHash()
                .multiGet(key + ":meta", userIds);

        // Zip: pair each (userId, score) tuple with its username at the same index.
        List<LeaderboardEntry> entries = new ArrayList<>(ordered.size());
        for (int i = 0; i < ordered.size(); i++) {
            String uid   = (String) userIds.get(i);
            double sc    = ordered.get(i).getScore() != null ? ordered.get(i).getScore() : 0.0;
            String uname = (String) usernames.get(i);   // null if userId not in meta hash

            entries.add(LeaderboardEntry.builder()
                    .rank((long) (offset + i + 1))       // offset+1 for 1-based rank display
                    .userId(uid != null ? uid : "")
                    .username(uname != null ? uname : uid) // fall back to userId if no username
                    .score(sc)
                    .build());
        }
        return entries;
    }

    /**
     * Look up a single user's rank, score, and display name.
     *
     * WHAT EACH REDIS COMMAND DOES:
     * ─────────────────────────────
     *   ZREVRANK key member
     *     → returns the 0-based rank of `member` counting from the highest
     *       score downward. Rank 0 = #1 player (highest score).
     *     → We add 1 before returning so the caller sees human-readable
     *       ranks starting at 1.
     *     → Returns null if the member is not in the set.
     *     → O(log N) complexity.
     *
     *   ZSCORE key member
     *     → returns the score of `member` as a double.
     *     → Returns null if the member is not in the set.
     *     → O(1) complexity.
     *
     *   HGET key field
     *     → returns the value stored at `field` in the hash at `key`.
     *     → Returns null if the field does not exist.
     *     → O(1) complexity.
     *
     * RTT IMPROVEMENT:
     * ─────────────────
     *   OLD approach: 3 sequential calls = 3 RTTs.
     *   NEW approach: 1 executePipelined  = 1 RTT.
     *   All three commands are pure reads with no dependency on each other,
     *   so they can safely be batched.
     *
     * HOW executePipelined WORKS HERE:
     * ─────────────────────────────────
     *   1. Spring puts the connection into buffer mode.
     *   2. The lambda sends zRevRank, zScore, hGet through the connection.
     *      None of these execute immediately — they queue in the buffer.
     *   3. Lambda returns null → Spring flushes the buffer in one TCP write.
     *   4. Redis executes all three commands and sends back three replies.
     *   5. Spring collects replies into List<Object> in dispatch order:
     *        results.get(0) → Long   (rank, from zRevRank)
     *        results.get(1) → Double (score, from zScore)
     *        results.get(2) → byte[] (username bytes, from hGet)
     *
     * WHY results.get(2) IS byte[] NOT String:
     * ──────────────────────────────────────────
     *   Inside a RedisCallback (the raw connection API), results come back
     *   as the wire-level types Redis actually returns — not Spring's
     *   deserialized objects. HGET returns raw bytes over the wire, so
     *   Spring gives us byte[] here. We convert with bytes2str().
     *
     *   ZSCORE  returns a double  over the wire → Double in Java. Fine.
     *   ZREVRANK returns an int   over the wire → Long   in Java. Fine.
     *   HGET    returns raw bytes over the wire → byte[] in Java. Needs conversion.
     */
    @Override
    public LeaderboardEntry getUserRank(String userId, String gameId) {
        String key = (gameId == null || gameId.isBlank())
                ? globalLeaderboardKey
                : gameLeaderboardPrefix + gameId;

        List<Object> results = stringRedisTemplate.executePipelined((RedisCallback<Object>) conn -> {
            conn.zSetCommands().zRevRank(bytes(key), bytes(userId));       // → Long
            conn.zSetCommands().zScore(bytes(key), bytes(userId));         // → Double
            conn.hashCommands().hGet(bytes(key + ":meta"), bytes(userId)); // → byte[]
            return null;
        });

        Long   rank  = (Long)   results.get(0);
        Double score = (Double) results.get(1);
        String uname = bytes2str((byte[]) results.get(2));

        return LeaderboardEntry.builder()
                .rank(rank != null ? rank + 1 : -1L)       // null = user not ranked yet
                .userId(userId)
                .username(uname != null ? uname : userId)   // fallback to userId if no name
                .score(score != null ? score : 0.0)
                .build();
    }

    /**
     * Total number of distinct players in the global leaderboard.
     *
     *   ZCARD key → returns the cardinality (number of members) of the ZSET.
     *   O(1) complexity — Redis tracks the count internally.
     */
    @Override
    public long getGlobalLeaderboardSize() {
        Long sz = stringRedisTemplate.opsForZSet().size(globalLeaderboardKey);
        return sz != null ? sz : 0L;
    }


    // ════════════════════════════════════════════════════════════════════════
    //  USER PROFILE
    // ════════════════════════════════════════════════════════════════════════

    /**
     * Persist a user profile.
     *
     * KEY DESIGN: user:profile:{username}
     *   Username is the lookup key, not userId. This means login is a single
     *   GET — no secondary index needed. See AUTH EVOLUTION comment in
     *   AuthService for the full history of why this changed.
     *
     * NO TTL intentionally:
     *   The previous design used a 24-hour TTL. This created a bug where the
     *   profile expired but a username:index HASH entry survived, making login
     *   return "Invalid credentials" for valid returning users after 24 hours.
     *   Profiles now live until explicitly deleted (account deletion).
     *
     *   Redis command: SET user:profile:alice '{"userId":"uid-abc123",...}'
     *   (no EX/PX suffix → no expiry)
     */
    @Override
    public void saveUserProfile(UserProfile profile) {
        userProfileTemplate.opsForValue()
                .set(userProfilePrefix + profile.getUsername(), profile);
        log.debug("[Redis] Saved profile username={}", profile.getUsername());
    }

    /**
     * Single-step login lookup.
     *
     *   Redis command: GET user:profile:{username}
     *   O(1) complexity.
     *
     *   Returns Optional.empty() if the key does not exist
     *   (username never registered, or profile deleted).
     */
    @Override
    public Optional<UserProfile> findProfileByUsername(String username) {
        return Optional.ofNullable(
                userProfileTemplate.opsForValue().get(userProfilePrefix + username));
    }

    /**
     * Check if a username is already taken during registration.
     *
     *   Redis command: EXISTS user:profile:{username}
     *   Returns 1 if the key exists, 0 if not. O(1).
     *   No need to deserialise the value — we only care about presence.
     */
    @Override
    public boolean usernameExists(String username) {
        return Boolean.TRUE.equals(userProfileTemplate.hasKey(userProfilePrefix + username));
    }

    /**
     * No-op. Secondary index was removed in v3.
     * The username IS the profile key — no separate mapping needed.
     * Method kept in the interface so that test verify(never()) calls compile.
     */
    @Override
    public void saveUsernameIndex(String username, String userId) { /* intentionally empty */ }


    // ════════════════════════════════════════════════════════════════════════
    //  PRIVATE HELPERS
    // ════════════════════════════════════════════════════════════════════════

    /**
     * Convert a Java String to a UTF-8 byte array.
     *
     * The low-level RedisConnection API (used inside executePipelined callbacks)
     * works with raw byte[] rather than typed objects. All key and field
     * arguments must be encoded as bytes before being sent to Redis.
     * UTF-8 is the encoding Spring Data Redis uses internally for String keys,
     * so we match that convention here.
     */
    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Convert a UTF-8 byte array back to a Java String.
     *
     * Null-safe: returns null if the input is null, which happens when
     * Redis returns a nil bulk-string reply (key or field not found).
     * Callers handle null by falling back to the userId string.
     */
    private static String bytes2str(byte[] b) {
        return b != null ? new String(b, StandardCharsets.UTF_8) : null;
    }
}