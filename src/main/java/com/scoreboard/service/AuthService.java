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
 * LOGIN : GET user:profile:alice → profile
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class AuthService {

    private final RedisLeaderboardRepository repository;
    private final PasswordEncoder            passwordEncoder;
    private final JwtUtil                    jwtUtil;

    // ── Registration ──────────────────────────────────────────────────────────

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

        // One write. Key = user:profile:{username}. No TTL. No secondary index.
        repository.saveUserProfile(profile);

        log.info("[Auth] Registered userId={} username={}", userId, username);
        return userId;
    }

    // ── Login ─────────────────────────────────────────────────────────────────

    public record LoginResult(String token, String userId) {}

    public LoginResult login(String username, String rawPassword) {
        // Single Redis GET: user:profile:{username}
        UserProfile profile = repository.findProfileByUsername(username)
                .orElseThrow(() -> {
                    log.warn("[Auth] No account found for username={}", username);
                    return new IllegalArgumentException("Invalid credentials");
                });

        if (!passwordEncoder.matches(rawPassword, profile.getPasswordHash())) {
            log.warn("[Auth] Wrong password for username={}", username);
            throw new IllegalArgumentException("Invalid credentials");
        }

        String token = jwtUtil.generateToken(profile.getUserId(), profile.getUsername());
        log.info("[Auth] Login success userId={}", profile.getUserId());
        return new LoginResult(token, profile.getUserId());
    }
}
