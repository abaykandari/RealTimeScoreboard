package com.scoreboard.security;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtException;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.authentication.WebAuthenticationDetailsSource;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.List;

/**
 * JWT Authentication Filter — THE MISSING PIECE that caused all 403 errors.
 *
 * HOW SPRING SECURITY WORKS (mental model):
 * ──────────────────────────────────────────
 * Spring Security processes every request through a filter chain.
 * If no filter places an Authentication object into SecurityContextHolder
 * by the end of that chain, the request is treated as ANONYMOUS.
 *
 * SecurityConfig says .anyRequest().authenticated()
 *   → "if SecurityContextHolder is empty → return 403"
 *
 * Your JWT was being sent correctly in the Authorization header.
 * But NOTHING was reading it and telling Spring Security "this is authenticated."
 * This class is that missing bridge.
 *
 * FLOW PER REQUEST:
 *   1. Read Authorization header, extract Bearer token
 *   2. Validate token signature and expiry via JwtUtil
 *   3. Parse userId + username from JWT claims
 *   4. Build UsernamePasswordAuthenticationToken (Spring's auth object)
 *   5. Set it in SecurityContextHolder
 *   6. Spring sees the request as authenticated → endpoint returns 200, not 403
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class JwtAuthenticationFilter extends OncePerRequestFilter {

    private final JwtUtil jwtUtil;

    private static final String AUTH_HEADER   = "Authorization";
    private static final String BEARER_PREFIX = "Bearer ";

    @Override
    protected void doFilterInternal(HttpServletRequest  request,
                                    HttpServletResponse response,
                                    FilterChain         chain)
            throws ServletException, IOException {

        // Step 1: read the header
        String header = request.getHeader(AUTH_HEADER);
        if (header == null || !header.startsWith(BEARER_PREFIX)) {
            chain.doFilter(request, response);
            return;
        }

        String token = header.substring(BEARER_PREFIX.length()).trim();

        // Step 2 + 3: validate and parse
        try {
            Claims claims = jwtUtil.validateAndParseClaims(token);

            // Step 4: only set auth if not already set (idempotency guard)
            if (SecurityContextHolder.getContext().getAuthentication() == null) {
                String userId = claims.getSubject();

                // Step 5: register the authenticated user with Spring Security
                UsernamePasswordAuthenticationToken auth =
                        new UsernamePasswordAuthenticationToken(
                                userId,   // principal
                                null,     // credentials (not needed after JWT validation)
                                List.of() // authorities — no roles in this project
                        );
                auth.setDetails(new WebAuthenticationDetailsSource().buildDetails(request));
                SecurityContextHolder.getContext().setAuthentication(auth);

                log.debug("[JWT] Authenticated userId={} path={}",
                        userId, request.getRequestURI());
            }
        } catch (JwtException e) {
            // Invalid or expired token → clear context → endpoint will return 403
            log.warn("[JWT] Rejected token for path={} reason={}",
                    request.getRequestURI(), e.getMessage());
            SecurityContextHolder.clearContext();
        }

        // Step 6: always continue the chain
        chain.doFilter(request, response);
    }

    /** Skip this filter for public paths — no token needed there */
    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        String path = request.getServletPath();
        return path.startsWith("/api/auth/")
            || path.startsWith("/actuator/")
            || path.startsWith("/ws");
    }
}
