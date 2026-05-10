package com.scoreboard.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Lightweight DTO for a single leaderboard row.
 * Assembled from Redis ZRANGE results before being serialized
 * into a gRPC / WebSocket response.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LeaderboardEntry {

    private long rank;
    private String userId;
    private String username;
    private double score;
}
