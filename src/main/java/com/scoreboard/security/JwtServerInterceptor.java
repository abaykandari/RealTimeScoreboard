package com.scoreboard.security;

import io.grpc.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.interceptor.GrpcGlobalServerInterceptor;

/**
 * gRPC server-side interceptor that validates JWT bearer tokens.
 *
 * Applied globally to all gRPC methods.
 * Unauthenticated calls are rejected with UNAUTHENTICATED status.
 *
 * Methods that should be public (Register, Login) are whitelisted
 * by full method name.
 */
@Slf4j
@GrpcGlobalServerInterceptor
@RequiredArgsConstructor
public class JwtServerInterceptor implements ServerInterceptor {

    /** Carry the authenticated userId downstream into the service */
    public static final Context.Key<String> USER_ID_KEY =
            Context.key("userId");

    public static final Metadata.Key<String> AUTHORIZATION_KEY =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

    private static final String BEARER_PREFIX = "Bearer ";

    private final JwtUtil jwtUtil;

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call,
            Metadata headers,
            ServerCallHandler<ReqT, RespT> next) {

        String fullMethod = call.getMethodDescriptor().getFullMethodName();

        // ── Public methods do not require a token ───────────────────────────
        if (isPublicMethod(fullMethod)) {
            return next.startCall(call, headers);
        }

        // ── Extract and validate token ──────────────────────────────────────
        String authHeader = headers.get(AUTHORIZATION_KEY);
        if (authHeader == null || !authHeader.startsWith(BEARER_PREFIX)) {
            log.warn("Missing or malformed Authorization header for method: {}", fullMethod);
            call.close(Status.UNAUTHENTICATED.withDescription("Missing bearer token"), new Metadata());
            return new ServerCall.Listener<>() {};
        }

        String token = authHeader.substring(BEARER_PREFIX.length()).trim();
        try {
            String userId = jwtUtil.extractUserId(token);
            // Propagate userId into gRPC Context so service methods can read it
            Context ctx = Context.current().withValue(USER_ID_KEY, userId);
            return Contexts.interceptCall(ctx, call, headers, next);
        } catch (Exception e) {
            log.warn("Invalid JWT token for method {}: {}", fullMethod, e.getMessage());
            call.close(Status.UNAUTHENTICATED.withDescription("Invalid or expired token"), new Metadata());
            return new ServerCall.Listener<>() {};
        }
    }

    private boolean isPublicMethod(String fullMethod) {
        return fullMethod.contains("Register") || fullMethod.contains("Login");
    }
}
