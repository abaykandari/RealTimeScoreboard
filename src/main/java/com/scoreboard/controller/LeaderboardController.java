package com.scoreboard.controller;

import com.scoreboard.model.LeaderboardEntry;
import com.scoreboard.service.LeaderboardService;
import com.scoreboard.service.ScoreService;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * REST controller for leaderboard queries and score submission.
 *
 * All endpoints require a valid JWT in the Authorization header.
 *
 * GET  /api/leaderboard/global
 * GET  /api/leaderboard/game/{gameId}
 * GET  /api/leaderboard/rank/{userId}
 * POST /api/scores
 */
@Slf4j
@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class LeaderboardController {

    private final LeaderboardService leaderboardService;
    private final ScoreService       scoreService;

    // ─── Leaderboard reads ───────────────────────────────────────────────────

    @GetMapping("/leaderboard/global")
    public ResponseEntity<Map<String, Object>> getGlobalLeaderboard(
            @RequestParam(defaultValue = "10") @Min(1) @Max(100) int limit,
            @RequestParam(defaultValue = "0")  @Min(0)           int offset) {

        List<LeaderboardEntry> entries = leaderboardService.getGlobalLeaderboard(limit, offset);
        return ResponseEntity.ok(Map.of(
                "entries", entries,
                "total",   leaderboardService.getGlobalLeaderboardSize()
        ));
    }

    @GetMapping("/leaderboard/game/{gameId}")
    public ResponseEntity<Map<String, Object>> getGameLeaderboard(
            @PathVariable String gameId,
            @RequestParam(defaultValue = "10") @Min(1) @Max(100) int limit,
            @RequestParam(defaultValue = "0")  @Min(0)           int offset) {

        List<LeaderboardEntry> entries = leaderboardService.getGameLeaderboard(gameId, limit, offset);
        return ResponseEntity.ok(Map.of("entries", entries));
    }

    @GetMapping("/leaderboard/rank/{userId}")
    public ResponseEntity<LeaderboardEntry> getUserRank(
            @PathVariable String userId,
            @RequestParam(defaultValue = "") String gameId) {

        LeaderboardEntry entry = leaderboardService.getUserRank(userId, gameId);
        return ResponseEntity.ok(entry);
    }

    // ─── Score submission ─────────────────────────────────────────────────────

    public record SubmitScoreRequest(
            String userId,
            String username,
            String gameId,
            double score
    ) {}

    @PostMapping("/scores")
    public ResponseEntity<Map<String, Object>> submitScore(
            @RequestBody SubmitScoreRequest req) {

        long rank = scoreService.submitScore(
                req.userId(), req.username(), req.gameId(), req.score());

        return ResponseEntity.accepted().body(Map.of(
                "accepted",    true,
                "currentRank", rank,
                "message",     "Score queued — leaderboard will update shortly"
        ));
    }
}
