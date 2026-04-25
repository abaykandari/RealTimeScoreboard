/*
package com.scoreboard.service;

import com.scoreboard.model.LeaderboardEntry;
import com.scoreboard.model.ScoreEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class LeaderboardServiceTest {

    @Mock RedisTemplate<String, String>                       redisTemplate;
    @Mock RedisTemplate<String, com.scoreboard.model.UserProfile> userProfileTemplate;
    @Mock ZSetOperations<String, String>                      zSetOps;

    @InjectMocks LeaderboardService service;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(service, "globalLeaderboardKey",   "leaderboard:global");
        ReflectionTestUtils.setField(service, "gameLeaderboardPrefix",   "leaderboard:game:");
        ReflectionTestUtils.setField(service, "userProfilePrefix",       "user:profile:");
        ReflectionTestUtils.setField(service, "userProfileTtlSeconds",   86400L);
    }

    @Test
    @DisplayName("getUserRank returns correct rank and score from Redis")
    void getUserRank_returnsCorrectData() {
        when(redisTemplate.opsForZSet()).thenReturn(zSetOps);
        when(zSetOps.reverseRank("leaderboard:global", "user-1")).thenReturn(0L);  // 0-indexed → rank 1
        when(zSetOps.score("leaderboard:global", "user-1")).thenReturn(9999.0);

        LeaderboardEntry entry = service.getUserRank("user-1", null);

        assertThat(entry.getRank()).isEqualTo(1L);
        assertThat(entry.getScore()).isEqualTo(9999.0);
        assertThat(entry.getUserId()).isEqualTo("user-1");
    }

    @Test
    @DisplayName("getGlobalLeaderboard returns ranked list in correct order")
    void getGlobalLeaderboard_returnsSortedEntries() {
        when(redisTemplate.opsForZSet()).thenReturn(zSetOps);

        Set<ZSetOperations.TypedTuple<String>> tuples = new LinkedHashSet<>();
        tuples.add(mockTuple("user-1", 9000.0));
        tuples.add(mockTuple("user-2", 7500.0));

        when(zSetOps.reverseRangeWithScores("leaderboard:global", 0, 9))
                .thenReturn(tuples);

        List<LeaderboardEntry> entries = service.getGlobalLeaderboard(10, 0);

        assertThat(entries).hasSize(2);
        assertThat(entries.get(0).getRank()).isEqualTo(1L);
        assertThat(entries.get(0).getScore()).isEqualTo(9000.0);
        assertThat(entries.get(1).getRank()).isEqualTo(2L);
    }

    private ZSetOperations.TypedTuple<String> mockTuple(String value, double score) {
        ZSetOperations.TypedTuple<String> tuple = mock(ZSetOperations.TypedTuple.class);
        when(tuple.getValue()).thenReturn(value);
        when(tuple.getScore()).thenReturn(score);
        return tuple;
    }
}
*/
