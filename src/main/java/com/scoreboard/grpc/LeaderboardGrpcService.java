package com.scoreboard.grpc;

import com.scoreboard.model.LeaderboardEntry;
import com.scoreboard.model.UserProfile;
import com.scoreboard.security.JwtServerInterceptor;
import com.scoreboard.service.*;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.Executor;

/**
 * gRPC service implementation.
 *
 * Implements all RPCs declared in leaderboard.proto:
 *   - Register / Login (auth, no token required)
 *   - SubmitScore     (publishes to Kafka)
 *   - GetGlobalLeaderboard / GetGameLeaderboard / GetUserRank  (Redis reads)
 *   - StreamLeaderboard (server-streaming — real-time push loop)
 *
 * The @GrpcService annotation registers this with the grpc-spring-boot-starter
 * embedded gRPC server (port 9090 by default).
 */
@Slf4j
@GrpcService
@RequiredArgsConstructor
public class LeaderboardGrpcService extends LeaderboardServiceGrpc.LeaderboardServiceImplBase {

    private final AuthService                 authService;
    private final ScoreService                scoreService;
    private final LeaderboardService          leaderboardService;
    private final LeaderboardStreamBroadcaster broadcaster;

    @Value("${app.leaderboard.stream-interval-ms}")
    private long streamIntervalMs;

    // ──────────────────────────────────────────────────────────────────────────
    //  Auth RPCs
    // ──────────────────────────────────────────────────────────────────────────

    @Override
    public void register(RegisterRequest request,
                         StreamObserver<RegisterResponse> responseObserver) {
        try {
            String userId = authService.register(
                    request.getUsername(),
                    request.getPassword(),
                    request.getEmail()
            );
            responseObserver.onNext(RegisterResponse.newBuilder()
                    .setUserId(userId)
                    .setMessage("Registration successful")
                    .build());
            responseObserver.onCompleted();
        } catch (IllegalArgumentException e) {
            responseObserver.onError(Status.ALREADY_EXISTS
                    .withDescription(e.getMessage())
                    .asRuntimeException());
        } catch (Exception e) {
            log.error("[gRPC] Register failed: {}", e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Registration failed")
                    .asRuntimeException());
        }
    }

    @Override
    public void login(LoginRequest request,
                      StreamObserver<LoginResponse> responseObserver) {
        try {
            AuthService.LoginResult result = authService.login(
                    request.getUsername(),
                    request.getPassword()
            );
            responseObserver.onNext(LoginResponse.newBuilder()
                    .setToken(result.token())
                    .setUserId(result.userId())
                    .build());
            responseObserver.onCompleted();
        } catch (IllegalArgumentException e) {
            responseObserver.onError(Status.UNAUTHENTICATED
                    .withDescription("Invalid username or password")
                    .asRuntimeException());
        } catch (Exception e) {
            log.error("[gRPC] Login failed: {}", e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Login failed")
                    .asRuntimeException());
        }
    }

    // ──────────────────────────────────────────────────────────────────────────
    //  Score submission
    // ──────────────────────────────────────────────────────────────────────────

    @Override
    public void submitScore(SubmitScoreRequest request,
                            StreamObserver<SubmitScoreResponse> responseObserver) {
        try {
            // userId may also come from the JWT context; validate ownership here
            String authenticatedUserId = JwtServerInterceptor.USER_ID_KEY.get();
            if (!authenticatedUserId.equals(request.getUserId())) {
                responseObserver.onError(Status.PERMISSION_DENIED
                        .withDescription("Cannot submit scores for another user")
                        .asRuntimeException());
                return;
            }

            long rank = scoreService.submitScore(
                    request.getUserId(),
                    request.getUsername(),
                    request.getGameId(),
                    request.getScore()
            );

            responseObserver.onNext(SubmitScoreResponse.newBuilder()
                    .setSuccess(true)
                    .setGlobalRank(rank)
                    .setMessage("Score accepted")
                    .build());
            responseObserver.onCompleted();

        } catch (IllegalArgumentException e) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription(e.getMessage())
                    .asRuntimeException());
        } catch (Exception e) {
            log.error("[gRPC] SubmitScore failed: {}", e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Score submission failed")
                    .asRuntimeException());
        }
    }

    // ──────────────────────────────────────────────────────────────────────────
    //  Leaderboard queries
    // ──────────────────────────────────────────────────────────────────────────

    @Override
    public void getGlobalLeaderboard(GetLeaderboardRequest request,
                                     StreamObserver<LeaderboardResponse> responseObserver) {
        try {
            int limit  = request.getLimit()  > 0 ? request.getLimit()  : 10;
            int offset = request.getOffset() > 0 ? request.getOffset() : 0;
            List<LeaderboardEntry> entries = leaderboardService.getGlobalLeaderboard(limit, offset);
            responseObserver.onNext(buildLeaderboardResponse(entries));
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("[gRPC] GetGlobalLeaderboard failed: {}", e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withDescription("Fetch failed").asRuntimeException());
        }
    }

    @Override
    public void getGameLeaderboard(GetLeaderboardRequest request,
                                   StreamObserver<LeaderboardResponse> responseObserver) {
        try {
            int limit  = request.getLimit()  > 0 ? request.getLimit()  : 10;
            int offset = request.getOffset() > 0 ? request.getOffset() : 0;
            List<LeaderboardEntry> entries = leaderboardService
                    .getGameLeaderboard(request.getGameId(), limit, offset);
            responseObserver.onNext(buildLeaderboardResponse(entries));
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("[gRPC] GetGameLeaderboard failed: {}", e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withDescription("Fetch failed").asRuntimeException());
        }
    }

    @Override
    public void getUserRank(GetUserRankRequest request,
                            StreamObserver<GetUserRankResponse> responseObserver) {
        try {
            LeaderboardEntry entry = leaderboardService.getUserRank(
                    request.getUserId(), request.getGameId());
            responseObserver.onNext(GetUserRankResponse.newBuilder()
                    .setRank(entry.getRank())
                    .setScore(entry.getScore())
                    .setUsername(entry.getUsername())
                    .build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("[gRPC] GetUserRank failed: {}", e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withDescription("Fetch failed").asRuntimeException());
        }
    }

    // ──────────────────────────────────────────────────────────────────────────
    //  Server-streaming RPC
    // ──────────────────────────────────────────────────────────────────────────

    /**
     * StreamLeaderboard — server-streaming RPC.
     *
     * The server keeps the stream open and pushes a fresh leaderboard snapshot
     * whenever the broadcaster signals an update (event-driven) or on the
     * periodic heartbeat (every streamIntervalMs ms).
     *
     * Client cancellation is handled via ServerCallStreamObserver.setOnCancelHandler.
     */
    @Override
    public void streamLeaderboard(StreamLeaderboardRequest request,
                                  StreamObserver<LeaderboardUpdate> rawObserver) {

        ServerCallStreamObserver<LeaderboardUpdate> observer =
                (ServerCallStreamObserver<LeaderboardUpdate>) rawObserver;

        String gameId = request.getGameId();
        int    topN   = request.getTopN() > 0 ? request.getTopN() : 10;

        log.info("[gRPC] StreamLeaderboard started gameId='{}' topN={}", gameId, topN);

        // ── Wrap the observer so the broadcaster can call typed push() ────────
        var delegate = new LeaderboardStreamBroadcaster.PushCapable() {
            @Override
            public void push(List<LeaderboardEntry> entries, String gId) {
                if (!observer.isCancelled()) {
                    observer.onNext(buildLeaderboardUpdate(gId, entries));
                }
            }
        };

        // Cast to StreamObserver for registry (PushCapable is a secondary interface)
        var wrappedObserver = new StreamObserver<LeaderboardUpdate>() {
            @Override public void onNext(LeaderboardUpdate value) { observer.onNext(value); }
            @Override public void onError(Throwable t) { observer.onError(t); }
            @Override public void onCompleted() { observer.onCompleted(); }

            // Tag with PushCapable for the broadcaster
            public boolean isPushCapable() { return true; }
            public void push(List<LeaderboardEntry> entries, String gId) { delegate.push(entries, gId); }
        };

        broadcaster.registerObserver(gameId, topN, wrappedObserver);

        // ── Handle client disconnect / cancel ─────────────────────────────────
        observer.setOnCancelHandler(() -> {
            log.info("[gRPC] StreamLeaderboard cancelled by client for gameId='{}'", gameId);
            broadcaster.removeObserver(gameId, wrappedObserver);
        });

        // ── Send an immediate snapshot so client sees data before first event ─
        try {
            List<LeaderboardEntry> initial = (gameId == null || gameId.isBlank())
                    ? leaderboardService.getGlobalLeaderboard(topN, 0)
                    : leaderboardService.getGameLeaderboard(gameId, topN, 0);
            observer.onNext(buildLeaderboardUpdate(gameId, initial));
        } catch (Exception e) {
            log.error("[gRPC] Failed to send initial snapshot for gameId='{}': {}", gameId, e.getMessage());
        }
    }

    // ──────────────────────────────────────────────────────────────────────────
    //  Proto builders
    // ──────────────────────────────────────────────────────────────────────────

    private LeaderboardResponse buildLeaderboardResponse(List<LeaderboardEntry> entries) {
        LeaderboardResponse.Builder builder = LeaderboardResponse.newBuilder()
                .setTotal(entries.size());
        for (LeaderboardEntry e : entries) {
            builder.addEntries(toProtoEntry(e));
        }
        return builder.build();
    }

    private LeaderboardUpdate buildLeaderboardUpdate(String gameId,
                                                     List<LeaderboardEntry> entries) {
        LeaderboardUpdate.Builder builder = LeaderboardUpdate.newBuilder()
                .setGameId(gameId != null ? gameId : "global")
                .setTimestamp(Instant.now().toEpochMilli())
                .setEventType("SCORE_UPDATE");
        for (LeaderboardEntry e : entries) {
            builder.addEntries(toProtoEntry(e));
        }
        return builder.build();
    }

    private com.scoreboard.grpc.LeaderboardEntry toProtoEntry(LeaderboardEntry e) {
        return com.scoreboard.grpc.LeaderboardEntry.newBuilder()
                .setRank(e.getRank())
                .setUserId(e.getUserId())
                .setUsername(e.getUsername())
                .setScore(e.getScore())
                .build();
    }
}
