package com.scoreboard.service;

import com.scoreboard.model.LeaderboardEntry;
import com.scoreboard.model.ScoreEvent;
import com.scoreboard.repository.RedisLeaderboardRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link LeaderboardService}.
 *
 * WHY THESE TESTS NOW WORK ON JAVA 21/25:
 * ─────────────────────────────────────────
 * The old tests mocked {@code RedisTemplate} — a concrete Spring class
 * with complex internal state.  Byte Buddy (used by Mockito's inline
 * mock maker) struggles to instrument Spring/JDK internal classes on
 * newer Java versions.
 *
 * After the refactor, {@code LeaderboardService} depends only on
 * {@code RedisLeaderboardRepository} — a plain Java interface.
 * Mockito can mock interfaces with ZERO Byte Buddy involvement;
 * it uses a simple JDK dynamic proxy instead.  No experimental flags
 * needed, no JVM restrictions hit.
 */
@ExtendWith(MockitoExtension.class)
class LeaderboardServiceTest {

    // ── Mocking a plain interface: always works, on any Java version ───────
    @Mock
    private RedisLeaderboardRepository repository;

    @InjectMocks
    private LeaderboardService service;

    // ──────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("updateScore delegates to repository with correct arguments")
    void updateScore_delegatesToRepository() {
        ScoreEvent event = ScoreEvent.builder()
                .userId("user-1")
                .username("alice")
                .gameId("chess")
                .score(9500.0)
                .build();

        service.updateScore(event);

        verify(repository, times(1))
                .updateScore("user-1", "alice", "chess", 9500.0);
    }

    @Test
    @DisplayName("getUserRank returns the entry provided by the repository")
    void getUserRank_returnsRepositoryResult() {
        LeaderboardEntry expected = LeaderboardEntry.builder()
                .rank(1L)
                .userId("user-1")
                .username("alice")
                .score(9500.0)
                .build();

        when(repository.getUserRank("user-1", null)).thenReturn(expected);

        LeaderboardEntry result = service.getUserRank("user-1", null);

        assertThat(result.getRank()).isEqualTo(1L);
        assertThat(result.getScore()).isEqualTo(9500.0);
        assertThat(result.getUsername()).isEqualTo("alice");
    }

    @Test
    @DisplayName("getGlobalLeaderboard returns ranked list from repository")
    void getGlobalLeaderboard_returnsRepositoryResult() {
        List<LeaderboardEntry> fakeBoard = List.of(
                LeaderboardEntry.builder().rank(1L).userId("u1").username("alice").score(9000.0).build(),
                LeaderboardEntry.builder().rank(2L).userId("u2").username("bob").score(7500.0).build()
        );

        when(repository.getGlobalLeaderboard(10, 0)).thenReturn(fakeBoard);

        List<LeaderboardEntry> result = service.getGlobalLeaderboard(10, 0);

        assertThat(result).hasSize(2);
        assertThat(result.get(0).getRank()).isEqualTo(1L);
        assertThat(result.get(0).getScore()).isEqualTo(9000.0);
        assertThat(result.get(1).getRank()).isEqualTo(2L);
    }

    @Test
    @DisplayName("getGlobalLeaderboardSize delegates to repository")
    void getGlobalLeaderboardSize_delegatesToRepository() {
        when(repository.getGlobalLeaderboardSize()).thenReturn(1523L);

        long size = service.getGlobalLeaderboardSize();

        assertThat(size).isEqualTo(1523L);
        verify(repository).getGlobalLeaderboardSize();
    }

    @Test
    @DisplayName("usernameExists delegates to repository")
    void usernameExists_delegatesToRepository() {
        when(repository.usernameExists("alice")).thenReturn(true);
        when(repository.usernameExists("nobody")).thenReturn(false);

        assertThat(service.usernameExists("alice")).isTrue();
        assertThat(service.usernameExists("nobody")).isFalse();
    }
}
