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

/**
 * AuthService unit tests — v3 (username-keyed profiles, single-step login).
 *
 * What changed from v2:
 *   - No more findUserIdByUsername() stub — that method no longer exists
 *   - findProfileByUsername("alice") replaces the two-step lookup
 *   - No saveUsernameIndex() call to verify — registration is one write now
 */
@ExtendWith(MockitoExtension.class)
class AuthServiceTest {

    @Mock RedisLeaderboardRepository repository;
    @Mock PasswordEncoder            passwordEncoder;
    @Mock JwtUtil                    jwtUtil;

    @InjectMocks AuthService authService;

    // ── Registration ──────────────────────────────────────────────────────────

    @Test
    @DisplayName("register: saves profile and returns a non-blank userId")
    void register_savesProfileAndReturnsUserId() {
        when(repository.usernameExists("alice")).thenReturn(false);
        when(passwordEncoder.encode("secret")).thenReturn("$hashed$");

        String userId = authService.register("alice", "secret", "alice@test.com");

        assertThat(userId).isNotBlank();

        ArgumentCaptor<UserProfile> captor = ArgumentCaptor.forClass(UserProfile.class);
        verify(repository).saveUserProfile(captor.capture());

        UserProfile saved = captor.getValue();
        assertThat(saved.getUsername()).isEqualTo("alice");
        assertThat(saved.getEmail()).isEqualTo("alice@test.com");
        assertThat(saved.getPasswordHash()).isEqualTo("$hashed$");

        // v3: no secondary index — saveUsernameIndex must NOT be called
        verify(repository, never()).saveUsernameIndex(any(), any());
    }

    @Test
    @DisplayName("register: throws when username already taken")
    void register_throwsWhenUsernameTaken() {
        when(repository.usernameExists("alice")).thenReturn(true);

        assertThatThrownBy(() -> authService.register("alice", "secret", "x@y.com"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("already taken");

        verify(repository, never()).saveUserProfile(any());
    }

    // ── Login ─────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("login: returns token on valid credentials")
    void login_returnsTokenOnValidCredentials() {
        UserProfile profile = UserProfile.builder()
                .userId("uid-1").username("alice").passwordHash("$hashed$").build();

        // v3: single stub — findProfileByUsername with the username string
        when(repository.findProfileByUsername("alice")).thenReturn(Optional.of(profile));
        when(passwordEncoder.matches("secret", "$hashed$")).thenReturn(true);
        when(jwtUtil.generateToken("uid-1", "alice")).thenReturn("jwt-token-123");

        AuthService.LoginResult result = authService.login("alice", "secret");

        assertThat(result.token()).isEqualTo("jwt-token-123");
        assertThat(result.userId()).isEqualTo("uid-1");
    }

    @Test
    @DisplayName("login: throws on wrong password")
    void login_throwsOnWrongPassword() {
        UserProfile profile = UserProfile.builder()
                .userId("uid-1").username("alice").passwordHash("$hashed$").build();

        when(repository.findProfileByUsername("alice")).thenReturn(Optional.of(profile));
        when(passwordEncoder.matches("wrong", "$hashed$")).thenReturn(false);

        assertThatThrownBy(() -> authService.login("alice", "wrong"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid credentials");

        verify(jwtUtil, never()).generateToken(any(), any());
    }

    @Test
    @DisplayName("login: throws when username does not exist")
    void login_throwsWhenUserNotFound() {
        // Profile key doesn't exist → Optional.empty()
        when(repository.findProfileByUsername("ghost")).thenReturn(Optional.empty());

        assertThatThrownBy(() -> authService.login("ghost", "pass"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid credentials");

        // Password check and JWT must never be reached
        verify(passwordEncoder, never()).matches(any(), any());
        verify(jwtUtil, never()).generateToken(any(), any());
    }

    @Test
    @DisplayName("login: never reaches password check when profile missing")
    void login_shortCircuitsOnMissingProfile() {
        when(repository.findProfileByUsername("alice")).thenReturn(Optional.empty());

        assertThatThrownBy(() -> authService.login("alice", "secret"))
                .isInstanceOf(IllegalArgumentException.class);

        verifyNoInteractions(passwordEncoder);
        verifyNoInteractions(jwtUtil);
    }
}
