## Frontend ↔ Backend integration

| Frontend action | Backend endpoint | Transport |
|---|---|---|
| Register | `POST /api/auth/register` | REST |
| Login | `POST /api/auth/login` | REST → JWT |
| Submit score | `POST /api/scores` | REST → Kafka → Redis |
| Global leaderboard | `GET /api/leaderboard/global` | REST |
| Game leaderboard | `GET /api/leaderboard/game/{gameId}` | REST |
| My rank | `GET /api/leaderboard/rank/{userId}` | REST |
| Live updates | `ws://localhost:8080/ws` STOMP | WebSocket |
| Subscribe topic | `/topic/leaderboard/global` | STOMP sub |
