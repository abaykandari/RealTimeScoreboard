#!/usr/bin/env python3
"""
gRPC demo client for the Real-Time Scoreboard system.

Prerequisites:
    pip install grpcio grpcio-tools

Generate stubs first (from project root):
    python -m grpc_tools.protoc \
        -I src/main/proto \
        --python_out=scripts \
        --grpc_python_out=scripts \
        src/main/proto/leaderboard.proto

Usage:
    # Start the server stack first:
    docker-compose up -d

    # Run this script:
    python scripts/demo_client.py
"""

import grpc
import sys
import time
import random

# Generated stubs (run protoc command above)
try:
    import leaderboard_pb2
    import leaderboard_pb2_grpc
except ImportError:
    print("ERROR: gRPC stubs not found. Run protoc first (see script header).")
    sys.exit(1)

SERVER_ADDR = "localhost:9090"


def create_channel():
    return grpc.insecure_channel(SERVER_ADDR)


def register_user(stub, username, password, email):
    req = leaderboard_pb2.RegisterRequest(
        username=username, password=password, email=email
    )
    resp = stub.Register(req)
    print(f"[Register] userId={resp.user_id}  message={resp.message}")
    return resp.user_id


def login(stub, username, password):
    req = leaderboard_pb2.LoginRequest(username=username, password=password)
    resp = stub.Login(req)
    print(f"[Login]    token={resp.token[:30]}...  userId={resp.user_id}")
    return resp.token, resp.user_id


def submit_score(stub, token, user_id, username, game_id, score):
    meta = [("authorization", f"Bearer {token}")]
    req = leaderboard_pb2.SubmitScoreRequest(
        user_id=user_id, username=username, game_id=game_id, score=score
    )
    resp = stub.SubmitScore(req, metadata=meta)
    print(f"[Score]    success={resp.success}  rank={resp.global_rank}  score={score:.0f}")


def stream_leaderboard(stub, token, game_id="", top_n=5):
    """Open a server-streaming RPC and print every update received."""
    meta = [("authorization", f"Bearer {token}")]
    req = leaderboard_pb2.StreamLeaderboardRequest(
        game_id=game_id, top_n=top_n, auth_token=token
    )
    print(f"\n[Stream]  Listening for leaderboard updates (game='{game_id or 'global'}')...")
    try:
        for update in stub.StreamLeaderboard(req, metadata=meta):
            print(f"\n  ── Update @ {update.timestamp} ─────────────────")
            for e in update.entries:
                print(f"    #{e.rank:<3}  {e.username:<15}  score={e.score:.0f}")
    except grpc.RpcError as ex:
        if ex.code() == grpc.StatusCode.CANCELLED:
            print("[Stream]  Client cancelled the stream.")
        else:
            print(f"[Stream]  Error: {ex.code()} — {ex.details()}")


def main():
    with create_channel() as channel:
        stub = leaderboard_pb2_grpc.LeaderboardServiceStub(channel)

        # ── 1. Register players ─────────────────────────────────────────────
        print("=" * 55)
        print("  Real-Time Scoreboard — gRPC Demo Client")
        print("=" * 55)

        players = [
            ("alice",   "secure_pw_1", "alice@example.com"),
            ("bob",     "secure_pw_2", "bob@example.com"),
            ("charlie", "secure_pw_3", "charlie@example.com"),
        ]

        tokens = {}
        user_ids = {}
        for username, pw, email in players:
            try:
                uid = register_user(stub, username, pw, email)
                user_ids[username] = uid
            except grpc.RpcError as e:
                print(f"[Register]  {username} already exists — skipping")

        # ── 2. Login ────────────────────────────────────────────────────────
        print()
        for username, pw, _ in players:
            token, uid = login(stub, username, pw)
            tokens[username] = token
            user_ids.setdefault(username, uid)

        # ── 3. Submit scores ────────────────────────────────────────────────
        print()
        game_id = "chess-championship"
        for i in range(5):
            for username, _, _ in players:
                score = random.uniform(1000, 10000)
                submit_score(
                    stub,
                    tokens[username],
                    user_ids[username],
                    username,
                    game_id,
                    score,
                )
            time.sleep(0.5)

        # ── 4. Stream updates ───────────────────────────────────────────────
        print()
        # Stream for 15 seconds, then exit
        import threading
        t = threading.Thread(
            target=stream_leaderboard,
            args=(stub, tokens["alice"], game_id, 5),
            daemon=True,
        )
        t.start()

        # Keep submitting scores while streaming
        for _ in range(6):
            time.sleep(2)
            for username, _, _ in players:
                score = random.uniform(5000, 15000)
                submit_score(
                    stub, tokens[username], user_ids[username], username, game_id, score
                )

        print("\n[Demo]  Done.")


if __name__ == "__main__":
    main()
