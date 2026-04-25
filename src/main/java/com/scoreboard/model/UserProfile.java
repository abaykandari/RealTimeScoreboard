package com.scoreboard.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.Instant;

/**
 * User profile stored in Redis as a JSON hash.
 * Key: user:profile:{userId}
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UserProfile implements Serializable {

    private String userId;
    private String username;
    private String email;
    private String passwordHash;

    @Builder.Default
    private long createdAt = Instant.now().toEpochMilli();
}
