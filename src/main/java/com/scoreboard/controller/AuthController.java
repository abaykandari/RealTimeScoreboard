package com.scoreboard.controller;

import com.scoreboard.service.AuthService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * REST controller for authentication.
 *
 * These endpoints are public (no JWT required) — see SecurityConfig.
 *
 * POST /api/auth/register
 * POST /api/auth/login
 *
 * Note: The primary API surface is gRPC. These REST endpoints exist as
 * a convenience for non-gRPC clients (e.g., browser, Postman).
 */
@Slf4j
@RestController
@RequestMapping("/api/auth")
@RequiredArgsConstructor
public class AuthController {

    private final AuthService authService;

    // ─── DTOs ────────────────────────────────────────────────────────────────

    public record RegisterRequest(
            @NotBlank @Size(min = 3, max = 30) String username,
            @NotBlank @Size(min = 8)           String password,
            @Email  @NotBlank                  String email
    ) {}

    public record LoginRequest(
            @NotBlank String username,
            @NotBlank String password
    ) {}

    // ─── Endpoints ───────────────────────────────────────────────────────────

    @PostMapping("/register")
    public ResponseEntity<?> register(@Valid @RequestBody RegisterRequest req) {
        try {
            String userId = authService.register(req.username(), req.password(), req.email());
            return ResponseEntity
                    .status(HttpStatus.CREATED)
                    .body(Map.of(
                            "userId",  userId,
                            "message", "Registration successful"
                    ));
        } catch (IllegalArgumentException e) {
            return ResponseEntity
                    .status(HttpStatus.CONFLICT)
                    .body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/login")
    public ResponseEntity<?> login(@Valid @RequestBody LoginRequest req) {
        try {
            AuthService.LoginResult result = authService.login(req.username(), req.password());
            return ResponseEntity.ok(Map.of(
                    "token",  result.token(),
                    "userId", result.userId()
            ));
        } catch (IllegalArgumentException e) {
            return ResponseEntity
                    .status(HttpStatus.UNAUTHORIZED)
                    .body(Map.of("error", "Invalid credentials"));
        }
    }
}
