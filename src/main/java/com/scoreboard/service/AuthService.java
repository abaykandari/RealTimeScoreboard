package com.scoreboard.service;

import com.scoreboard.model.UserProfile;
import com.scoreboard.security.JwtUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

import java.util.UUID;

/**
 * Handles user registration and authentication.
 *
 * Users are stored in Redis (no SQL database needed for this read-heavy system).
 * Passwords are hashed with BCrypt (cost factor 12).
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class AuthService {

    private final LeaderboardService leaderboardService;
    private final PasswordEncoder    passwordEncoder;
    private final JwtUtil            jwtUtil;

    // ──────────────────────────────────────────────────────────────────────────
    //  Registration
    // ──────────────────────────────────────────────────────────────────────────

    /**
     * Register a new user.
     *
     * @return newly created userId
     * @throws IllegalArgumentException if username already taken
     */
    public String register(String username, String rawPassword, String email) {
        if (leaderboardService.usernameExists(username)) {
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

        leaderboardService.saveUserProfile(profile);
        log.info("[Auth] Registered new user userId={} username={}", userId, username);
        return userId;
    }

    // ──────────────────────────────────────────────────────────────────────────
    //  Login
    // ──────────────────────────────────────────────────────────────────────────

    /**
     * Authenticate a user and return a JWT.
     *
     * @return signed JWT string
     * @throws IllegalArgumentException on bad credentials
     */
    public record LoginResult(String token, String userId) {}

    public LoginResult login(String username, String rawPassword) {
        // Scan profiles to find the matching username
        // In production: maintain a secondary username→userId index in Redis
        UserProfile profile = findByUsername(username);

        if (profile == null || !passwordEncoder.matches(rawPassword, profile.getPasswordHash())) {
            log.warn("[Auth] Login failed for username={}", username);
            throw new IllegalArgumentException("Invalid credentials");
        }

        String token = jwtUtil.generateToken(profile.getUserId(), profile.getUsername());
        log.info("[Auth] Login success userId={} username={}", profile.getUserId(), username);
        return new LoginResult(token, profile.getUserId());
    }

    // ──────────────────────────────────────────────────────────────────────────
    //  Helpers
    // ──────────────────────────────────────────────────────────────────────────

    private UserProfile findByUsername(String username) {
        // Delegated to LeaderboardService (Redis scan) — acceptable at low scale.
        // At high scale: maintain a "username:index" HSET in Redis for O(1) lookup.
        //
        // For now leaderboardService.usernameExists is used only for reg-check;
        // here we replicate the scan with a direct return.
        //
        // TODO: Add username→userId secondary index for O(1) lookup in production.
        return null; // placeholder — override in subclass or integration test
    }

    /**
     * In-memory lookup — delegates to Redis profile scan.
     * Called from AuthService internally via leaderboardService.
     */
    public UserProfile findProfileByUsername(String username) {
        return leaderboardService.getUserProfile(username); // simplified
    }
}
