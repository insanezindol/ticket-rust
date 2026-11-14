![header](https://capsule-render.vercel.app/api?type=wave&color=auto&height=300&section=header&text=ticket%20rust&fontSize=90)

# ticket-rust

Rust로 백엔드를 짜 보면 어떤 느낌인지 궁금해서 만든 작은 선착순 쿠폰 예약 API입니다.

주제로 "선착순"을 고른 건 동시에 요청이 몰리는 상황이 있어야 Rust의 async와 동시성 처리를 제대로 써 볼 수 있을 것 같아서였습니다.

## 써 보고 싶었던 것

- **Axum + Tokio**: 핸들러가 그냥 async 함수이고, 요청 파싱을 extractor(`Path`, `Query`, `Json`, `State`)가 타입으로 처리하는 방식
- **SQLx**: ORM 없이 SQL을 직접 쓰면서 트랜잭션, 락, 유니크 제약 위반 같은 DB 에러를 `Result`로 다루는 방식
- **에러 처리**: 예외 대신 `Result`와 `?`로 흘려보낼 때 코드가 어떻게 생기는지
- **enum + serde**: `?strategy=db_lock` 같은 쿼리 값을 enum으로 받아서 `match`로 빠짐없이 분기하기
- **동시성 테스트**: `JoinSet`으로 요청 수백 개를 동시에 쏴서 재고가 정확히 맞는지 확인하기

## 하는 일

쿠폰을 만들고 선착순으로 예약합니다. 재고를 줄이는 방법을 세 가지로 구현해 두고 `?strategy=` 쿼리로 골라 쓸 수 있습니다.

- `db_lock`: `SELECT … FOR UPDATE`로 행을 잠그고 차감
- `db_atomic`: `UPDATE … WHERE remaining > 0` 한 문장으로 확인과 차감을 같이 처리
- `redis`: Redis Lua 스크립트로 확인과 차감을 원자적으로 처리한 뒤 DB에 기록

추가로 한 사람당 한 장만 예약되게 막고, `Idempotency-Key` 헤더로 같은 요청이 다시 와도 한 번만 처리되게 했습니다.

## 실행

```bash
docker compose up -d
cargo run                                  # http://localhost:3000
cargo test -- --ignored --test-threads=1   # Postgres, Redis 필요
```

로컬에 Rust가 없으면 컨테이너에서 테스트할 수 있습니다.

```bash
docker run --rm --network ticket-rust_default -e DATABASE_URL=postgres://postgres:postgres@postgres:5432/ticket -e REDIS_URL=redis://redis:6379 -v "$PWD:/app" -v ticket-cargo:/usr/local/cargo/registry -v ticket-target:/app/target -w /app rust:1 cargo test -- --ignored --test-threads=1
```

## API

| 메서드 | 경로 | 설명 |
|---|---|---|
| POST | `/coupons` | `{"name":"..","stock":100}`로 쿠폰 생성 |
| GET | `/coupons/{id}` | 남은 재고와 예약 수 조회 |
| POST | `/coupons/{id}/reserve?strategy=db_lock\|db_atomic\|redis` | `Idempotency-Key` 헤더와 `{"user_id":1}`로 예약 |

```bash
curl -X POST localhost:3000/coupons -H 'content-type: application/json' -d '{"name":"오픈 기념","stock":100}'
curl -X POST 'localhost:3000/coupons/1/reserve?strategy=redis' \
  -H 'content-type: application/json' -H 'Idempotency-Key: abc-123' -d '{"user_id":42}'
```

## 테스트

`tests/concurrency.rs`

- 재고 50개에 200명이 동시에 요청하면 세 방식 모두 정확히 50명만 성공하는지
- 같은 사람이 동시에 여러 번 눌러도 한 장만 예약되는지
- 같은 `Idempotency-Key`로 다시 보내면 같은 응답이 오고 재고는 한 번만 줄어드는지

## 스택

Rust · Axum 0.8 · Tokio · SQLx 0.8 · PostgreSQL 16 · Redis 7
