package com.scoreboard.service;

import com.scoreboard.model.UserProfile;
import com.scoreboard.repository.RedisLeaderboardRepository;
import com.scoreboard.security.JwtUtil;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.crypto.password.PasswordEncoder;

import java.util.Optional;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AuthServiceTest {

    @Mock RedisLeaderboardRepository repository;
    @Mock PasswordEncoder            passwordEncoder;
    @Mock JwtUtil                    jwtUtil;

    @InjectMocks AuthService authService;

    // ── Registration tests ────────────────

    @Test
    @DisplayName("register: saves profile and returns a non-blank userId")
    void register_savesProfileAndReturnsUserId() {
        when(repository.usernameExists("alice")).thenReturn(false);
        when(passwordEncoder.encode("secret")).thenReturn("$hashed$");

        String userId = authService.register("alice", "secret", "alice@test.com");

        assertThat(userId).isNotBlank();

        // Verify the profile was saved with correct fields
        ArgumentCaptor<UserProfile> captor = ArgumentCaptor.forClass(UserProfile.class);
        verify(repository).saveUserProfile(captor.capture());
        UserProfile saved = captor.getValue();
        assertThat(saved.getUsername()).isEqualTo("alice");
        assertThat(saved.getEmail()).isEqualTo("alice@test.com");
        assertThat(saved.getPasswordHash()).isEqualTo("$hashed$");

        // Verify the secondary index was also written
        verify(repository).saveUsernameIndex("alice", userId);
    }

    @Test
    @DisplayName("register: throws when username is already taken")
    void register_throwsWhenUsernameTaken() {
        when(repository.usernameExists("alice")).thenReturn(true);

        assertThatThrownBy(() -> authService.register("alice", "secret", "x@y.com"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("already taken");

        verify(repository, never()).saveUserProfile(any());
        verify(repository, never()).saveUsernameIndex(any(), any());
    }

    // ── Login tests ───────────────────

    @Test
    @DisplayName("login: returns token on valid credentials")
    void login_returnsTokenOnValidCredentials() {
        // Arrange: build the profile that Redis would return
        UserProfile profile = UserProfile.builder()
                .userId("uid-1")
                .username("alice")
                .passwordHash("$hashed$")
                .build();

        // stub BOTH steps of the two-step login lookup
        when(repository.findUserIdByUsername("alice")).thenReturn("uid-1");  // Step 1
        when(repository.findUserProfile("uid-1")).thenReturn(Optional.of(profile)); // Step 2
        when(passwordEncoder.matches("secret", "$hashed$")).thenReturn(true);
        when(jwtUtil.generateToken("uid-1", "alice")).thenReturn("jwt-token-123");

        // Act
        AuthService.LoginResult result = authService.login("alice", "secret");

        // Assert
        assertThat(result.token()).isEqualTo("jwt-token-123");
        assertThat(result.userId()).isEqualTo("uid-1");
    }

    @Test
    @DisplayName("login: throws on wrong password")
    void login_throwsOnWrongPassword() {
        UserProfile profile = UserProfile.builder()
                .userId("uid-1").username("alice").passwordHash("$hashed$").build();

        // stub step 1 (username → userId) AND step 2 (userId → profile)
        // The old test only stubbed findUserProfile("alice") which never matched
        // because the service calls findUserProfile("uid-1") with the userId.
        when(repository.findUserIdByUsername("alice")).thenReturn("uid-1");
        when(repository.findUserProfile("uid-1")).thenReturn(Optional.of(profile));
        when(passwordEncoder.matches("wrong", "$hashed$")).thenReturn(false);

        assertThatThrownBy(() -> authService.login("alice", "wrong"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid credentials");

        // JWT should never be generated when password is wrong
        verify(jwtUtil, never()).generateToken(any(), any());
    }

    @Test
    @DisplayName("login: throws when username does not exist")
    void login_throwsWhenUserNotFound() {
        // stub step 1 to return null (username not in secondary index).
        // The old test stubbed findUserProfile("ghost") which was never called —
        // the service short-circuits at step 1 when findUserIdByUsername returns null.
        when(repository.findUserIdByUsername("ghost")).thenReturn(null);

        assertThatThrownBy(() -> authService.login("ghost", "pass"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid credentials");

        // Step 2 should never be reached — no profile lookup for unknown usernames
        verify(repository, never()).findUserProfile(any());
        verify(jwtUtil, never()).generateToken(any(), any());
    }

    @Test
    @DisplayName("login: throws when userId found but profile missing (data inconsistency)")
    void login_throwsWhenProfileMissing() {
        // Edge case: secondary index has the entry but profile key has expired in Redis.
        // This can happen if user:profile TTL expires but username:index does not.
        when(repository.findUserIdByUsername("alice")).thenReturn("uid-1");
        when(repository.findUserProfile("uid-1")).thenReturn(Optional.empty());

        assertThatThrownBy(() -> authService.login("alice", "anypass"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid credentials");

        verify(passwordEncoder, never()).matches(any(), any());
        verify(jwtUtil, never()).generateToken(any(), any());
    }
}
