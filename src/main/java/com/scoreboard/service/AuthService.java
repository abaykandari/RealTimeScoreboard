package com.scoreboard.service;

import com.scoreboard.model.UserProfile;
import com.scoreboard.repository.RedisLeaderboardRepository;
import com.scoreboard.security.JwtUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

import java.util.UUID;

/**
 * Handles user registration and authentication.
 *
 * KEY FIX: The original AuthService.findByUsername() always returned null
 * (hardcoded placeholder). Login always failed even for correct passwords.
 *
 * ROOT CAUSE:
 *   Redis profile key is "user:profile:{userId}" — NOT "user:profile:{username}".
 *   The original code passed username where userId was expected, so the key
 *   lookup always missed.
 *
 * THE FIX — two-step lookup with a secondary index:
 *   On register: HSET username:index <username> <userId>
 *   On login:    HGET username:index <username>  → userId → profile fetch
 *
 * Both operations are O(1). No full Redis key scan needed.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class AuthService {

    private final RedisLeaderboardRepository repository;
    private final PasswordEncoder            passwordEncoder;
    private final JwtUtil                    jwtUtil;

    // ── Registration ──────────────────────────────────────────────────────

    public String register(String username, String rawPassword, String email) {
        if (repository.usernameExists(username)) {
            throw new IllegalArgumentException("Username already taken: " + username);
        }

        String userId       = UUID.randomUUID().toString();
        String passwordHash = passwordEncoder.encode(rawPassword);

        UserProfile profile = UserProfile.builder()
                .userId(userId)
                .username(username)
                .email(email)
                .passwordHash(passwordHash)
                .build();

        // 1. Save the full profile at:  user:profile:{userId}
        repository.saveUserProfile(profile);

        // 2. Write secondary index:     username:index HSET username → userId
        //    Without this, login has no way to go from a username to a profile.
        repository.saveUsernameIndex(username, userId);

        log.info("[Auth] Registered userId={} username={}", userId, username);
        return userId;
    }

    // ── Login ─────────────────────────────────────────────────────────────

    public record LoginResult(String token, String userId) {}

    public LoginResult login(String username, String rawPassword) {
        // Step 1: username → userId  via secondary index (O(1) HGET)
        String userId = repository.findUserIdByUsername(username);
        if (userId == null) {
            log.warn("[Auth] Username not found: {}", username);
            throw new IllegalArgumentException("Invalid credentials");
        }

        // Step 2: userId → full profile  (O(1) GET on "user:profile:{userId}")
        UserProfile profile = repository.findUserProfile(userId)
                .orElseThrow(() -> new IllegalArgumentException("Invalid credentials"));

        // Step 3: verify BCrypt hash
        if (!passwordEncoder.matches(rawPassword, profile.getPasswordHash())) {
            log.warn("[Auth] Wrong password for username={}", username);
            throw new IllegalArgumentException("Invalid credentials");
        }

        // Step 4: issue signed JWT
        String token = jwtUtil.generateToken(profile.getUserId(), profile.getUsername());
        log.info("[Auth] Login success userId={}", userId);
        return new LoginResult(token, userId);
    }
}
