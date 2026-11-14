use std::sync::LazyLock;

use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
    Json, Router,
};
use redis::{aio::ConnectionManager, AsyncCommands};
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::{postgres::PgPoolOptions, PgConnection, PgPool};

#[derive(Clone)]
pub struct AppState {
    pub db: PgPool,
    pub redis: ConnectionManager,
}

pub async fn connect() -> Result<AppState, Box<dyn std::error::Error>> {
    let db_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/ticket".into());
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
    let db = PgPoolOptions::new().max_connections(20).connect(&db_url).await?;
    sqlx::migrate!().run(&db).await?;
    let redis = ConnectionManager::new(redis::Client::open(redis_url)?).await?;
    Ok(AppState { db, redis })
}

pub fn app(state: AppState) -> Router {
    Router::new()
        .route("/coupons", post(create_coupon))
        .route("/coupons/{id}", get(get_coupon))
        .route("/coupons/{id}/reserve", post(reserve))
        .with_state(state)
}

type Resp = (StatusCode, Json<Value>);
type ApiResult = Result<Resp, Resp>;

fn err(status: StatusCode, msg: &str) -> Resp {
    (status, Json(json!({ "error": msg })))
}

fn internal(e: impl std::fmt::Display) -> Resp {
    eprintln!("internal error: {e}");
    err(StatusCode::INTERNAL_SERVER_ERROR, "internal error")
}

fn stock_key(coupon: i64) -> String {
    format!("coupon:{coupon}:stock")
}

fn users_key(coupon: i64) -> String {
    format!("coupon:{coupon}:users")
}

// ---------- 쿠폰 생성 / 조회 ----------

#[derive(Deserialize)]
struct CreateCoupon {
    name: String,
    stock: i32,
}

async fn create_coupon(State(s): State<AppState>, Json(req): Json<CreateCoupon>) -> ApiResult {
    if req.stock < 0 {
        return Err(err(StatusCode::BAD_REQUEST, "stock must be >= 0"));
    }
    let id: i64 = sqlx::query_scalar(
        "INSERT INTO coupons (name, total_stock, remaining) VALUES ($1, $2, $2) RETURNING id",
    )
    .bind(&req.name)
    .bind(req.stock)
    .fetch_one(&s.db)
    .await
    .map_err(internal)?;

    // shortcut: Redis 재고는 생성 시 1회만 세팅 — Redis 재시작/유실 시 재고가 사라짐. 운영이면 AOF + DB 기준 재적재 작업 필요.
    let mut r = s.redis.clone();
    let _: () = r.set(stock_key(id), req.stock).await.map_err(internal)?;

    Ok((StatusCode::CREATED, Json(json!({ "id": id, "name": req.name, "stock": req.stock }))))
}

async fn get_coupon(State(s): State<AppState>, Path(id): Path<i64>) -> ApiResult {
    let row: Option<(String, i32, i32, i64)> = sqlx::query_as(
        "SELECT name, total_stock, remaining,
                (SELECT count(*) FROM reservations WHERE coupon_id = c.id)
         FROM coupons c WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(&s.db)
    .await
    .map_err(internal)?;
    let (name, total, remaining, reserved) = row.ok_or_else(|| err(StatusCode::NOT_FOUND, "coupon not found"))?;

    let mut r = s.redis.clone();
    let redis_remaining: Option<i64> = r.get(stock_key(id)).await.map_err(internal)?;

    Ok((
        StatusCode::OK,
        Json(json!({
            "id": id, "name": name, "total_stock": total,
            "remaining": remaining, "reserved": reserved, "redis_remaining": redis_remaining,
        })),
    ))
}

// ---------- 예약 ----------

#[derive(Deserialize, Default, Clone, Copy)]
#[serde(rename_all = "snake_case")]
enum Strategy {
    #[default]
    DbLock, // SELECT ... FOR UPDATE (비관적 락)
    DbAtomic, // UPDATE ... WHERE remaining > 0 (조건부 원자 UPDATE)
    Redis, // Lua 스크립트 원자 연산 후 DB 기록
}

#[derive(Deserialize)]
struct ReserveQuery {
    #[serde(default)]
    strategy: Strategy,
}

#[derive(Deserialize)]
struct ReserveReq {
    user_id: i64,
}

const IDEM_IN_FLIGHT_TTL: u64 = 30;
const IDEM_RESULT_TTL: u64 = 60 * 60 * 24;

/// 멱등성 키: 같은 키로 다시 오면 처리하지 않고 저장된 응답을 그대로 돌려준다(네트워크 재시도 대비).
/// 중복 요청 방지(1인 1매)는 별개 — DB UNIQUE(coupon_id, user_id) / Redis SET 이 담당.
async fn reserve(
    State(s): State<AppState>,
    Path(coupon): Path<i64>,
    Query(q): Query<ReserveQuery>,
    headers: HeaderMap,
    Json(req): Json<ReserveReq>,
) -> ApiResult {
    let key = headers
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .filter(|k| !k.is_empty() && k.len() <= 100)
        .ok_or_else(|| err(StatusCode::BAD_REQUEST, "Idempotency-Key header required (1..=100 chars)"))?
        .to_owned();
    let idem = format!("idem:{key}");
    let fingerprint = format!("{coupon}:{}", req.user_id);
    let mut r = s.redis.clone();

    // SET NX 로 키 선점. 실패 = 처리 중이거나 이미 끝난 요청.
    let claimed: Option<String> = redis::cmd("SET")
        .arg(&idem)
        .arg("")
        .arg("NX")
        .arg("EX")
        .arg(IDEM_IN_FLIGHT_TTL)
        .query_async(&mut r)
        .await
        .map_err(internal)?;
    if claimed.is_none() {
        let stored: Option<String> = r.get(&idem).await.map_err(internal)?;
        let Some(raw) = stored.filter(|v| !v.is_empty()) else {
            return Err(err(StatusCode::CONFLICT, "request with this Idempotency-Key is in progress"));
        };
        let saved: Value = serde_json::from_str(&raw).map_err(internal)?;
        if saved["fingerprint"] != fingerprint.as_str() {
            return Err(err(StatusCode::UNPROCESSABLE_ENTITY, "Idempotency-Key reused with a different request"));
        }
        let status = StatusCode::from_u16(saved["status"].as_u64().unwrap_or(500) as u16).map_err(internal)?;
        return Ok((status, Json(saved["body"].clone())));
    }

    let result = match q.strategy {
        Strategy::DbLock => reserve_db_lock(&s.db, coupon, req.user_id, &key).await,
        Strategy::DbAtomic => reserve_db_atomic(&s.db, coupon, req.user_id, &key).await,
        Strategy::Redis => reserve_redis(&s, coupon, req.user_id, &key).await,
    };
    let (status, Json(body)) = match result {
        Ok(r) | Err(r) => r,
    };

    if status.is_server_error() {
        // 서버 오류는 저장하지 않음 → 같은 키로 재시도 가능
        let _: redis::RedisResult<()> = r.del(&idem).await;
    } else {
        let saved = json!({ "fingerprint": fingerprint, "status": status.as_u16(), "body": body });
        let _: () = r.set_ex(&idem, saved.to_string(), IDEM_RESULT_TTL).await.map_err(internal)?;
    }
    Ok((status, Json(body)))
}

fn reserved(reservation_id: i64, coupon: i64, user: i64, remaining: i64) -> Resp {
    (
        StatusCode::OK,
        Json(json!({ "reservation_id": reservation_id, "coupon_id": coupon, "user_id": user, "remaining": remaining })),
    )
}

async fn insert_reservation(conn: &mut PgConnection, coupon: i64, user: i64, key: &str) -> Result<i64, Resp> {
    sqlx::query_scalar(
        "INSERT INTO reservations (coupon_id, user_id, idempotency_key) VALUES ($1, $2, $3) RETURNING id",
    )
    .bind(coupon)
    .bind(user)
    .bind(key)
    .fetch_one(conn)
    .await
    .map_err(|e| match &e {
        sqlx::Error::Database(d) if d.is_unique_violation() => err(StatusCode::CONFLICT, "already reserved"),
        _ => internal(e),
    })
}

/// 1) 비관적 락: 행을 잠그고 읽은 뒤 판단. 커밋까지 다른 트랜잭션은 대기.
async fn reserve_db_lock(db: &PgPool, coupon: i64, user: i64, key: &str) -> ApiResult {
    let mut tx = db.begin().await.map_err(internal)?;
    let remaining: Option<i32> = sqlx::query_scalar("SELECT remaining FROM coupons WHERE id = $1 FOR UPDATE")
        .bind(coupon)
        .fetch_optional(&mut *tx)
        .await
        .map_err(internal)?;
    let remaining = remaining.ok_or_else(|| err(StatusCode::NOT_FOUND, "coupon not found"))?;
    if remaining <= 0 {
        return Err(err(StatusCode::CONFLICT, "sold out"));
    }
    let id = insert_reservation(&mut *tx,coupon, user, key).await?; // 실패 시 tx drop → 롤백
    sqlx::query("UPDATE coupons SET remaining = remaining - 1 WHERE id = $1")
        .bind(coupon)
        .execute(&mut *tx)
        .await
        .map_err(internal)?;
    tx.commit().await.map_err(internal)?;
    Ok(reserved(id, coupon, user, (remaining - 1) as i64))
}

/// 2) 조건부 원자 UPDATE: 판단과 차감을 한 문장으로. 락 보유 시간이 짧고 왕복 1회.
async fn reserve_db_atomic(db: &PgPool, coupon: i64, user: i64, key: &str) -> ApiResult {
    let mut tx = db.begin().await.map_err(internal)?;
    let remaining: Option<i32> = sqlx::query_scalar(
        "UPDATE coupons SET remaining = remaining - 1 WHERE id = $1 AND remaining > 0 RETURNING remaining",
    )
    .bind(coupon)
    .fetch_optional(&mut *tx)
    .await
    .map_err(internal)?;
    let Some(remaining) = remaining else {
        let exists: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM coupons WHERE id = $1)")
            .bind(coupon)
            .fetch_one(&mut *tx)
            .await
            .map_err(internal)?;
        return Err(if exists { err(StatusCode::CONFLICT, "sold out") } else { err(StatusCode::NOT_FOUND, "coupon not found") });
    };
    let id = insert_reservation(&mut *tx,coupon, user, key).await?; // 중복이면 롤백 → 차감도 취소
    tx.commit().await.map_err(internal)?;
    Ok(reserved(id, coupon, user, remaining as i64))
}

// KEYS[1]=재고, KEYS[2]=당첨 유저 SET, ARGV[1]=user_id. Redis 는 단일 스레드라 스크립트 전체가 원자적.
static RESERVE_LUA: LazyLock<redis::Script> = LazyLock::new(|| {
    redis::Script::new(
        r#"
        if redis.call('SISMEMBER', KEYS[2], ARGV[1]) == 1 then return -2 end
        local stock = tonumber(redis.call('GET', KEYS[1]) or '-3')
        if stock == -3 then return -3 end
        if stock <= 0 then return -1 end
        redis.call('DECR', KEYS[1])
        redis.call('SADD', KEYS[2], ARGV[1])
        return stock - 1
        "#,
    )
});

/// 3) Redis 원자 연산: 재고 판정을 Redis 에서 끝내고, 통과한 요청(최대 재고 수)만 DB 에 기록.
async fn reserve_redis(s: &AppState, coupon: i64, user: i64, key: &str) -> ApiResult {
    let mut r = s.redis.clone();
    let left: i64 = RESERVE_LUA
        .key(stock_key(coupon))
        .key(users_key(coupon))
        .arg(user)
        .invoke_async(&mut r)
        .await
        .map_err(internal)?;
    match left {
        -3 => return Err(err(StatusCode::NOT_FOUND, "coupon not found")),
        -2 => return Err(err(StatusCode::CONFLICT, "already reserved")),
        -1 => return Err(err(StatusCode::CONFLICT, "sold out")),
        _ => {}
    }

    match persist_redis_winner(&s.db, coupon, user, key).await {
        Ok(id) => Ok(reserved(id, coupon, user, left)),
        Err(e) => {
            // 보상: DB 기록 실패 시 Redis 차감 되돌림.
            // shortcut: 보상 자체가 실패하면 재고 1개가 묶임. 운영이면 outbox/큐로 비동기 정합성 맞추기.
            let _: redis::RedisResult<()> = redis::pipe()
                .atomic()
                .incr(stock_key(coupon), 1)
                .srem(users_key(coupon), user)
                .query_async(&mut r)
                .await;
            Err(e)
        }
    }
}

async fn persist_redis_winner(db: &PgPool, coupon: i64, user: i64, key: &str) -> Result<i64, Resp> {
    let mut tx = db.begin().await.map_err(internal)?;
    let id = insert_reservation(&mut *tx,coupon, user, key).await?;
    sqlx::query("UPDATE coupons SET remaining = remaining - 1 WHERE id = $1")
        .bind(coupon)
        .execute(&mut *tx)
        .await
        .map_err(internal)?;
    tx.commit().await.map_err(internal)?;
    Ok(id)
}
