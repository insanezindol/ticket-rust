//! Postgres + Redis 필요: `docker compose up -d` 후 `cargo test -- --ignored --test-threads=1`
use std::time::{SystemTime, UNIX_EPOCH};

use axum::{body::Body, http::Request, Router};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use tower::ServiceExt;

async fn app() -> Router {
    ticket_rust::app(ticket_rust::connect().await.expect("start postgres/redis: docker compose up -d"))
}

fn run_id() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
}

async fn call(app: &Router, method: &str, uri: &str, idem: Option<&str>, body: Value) -> (u16, Value) {
    let mut req = Request::builder().method(method).uri(uri).header("content-type", "application/json");
    if let Some(k) = idem {
        req = req.header("idempotency-key", k);
    }
    let res = app.clone().oneshot(req.body(Body::from(body.to_string())).unwrap()).await.unwrap();
    let status = res.status().as_u16();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
}

async fn create_coupon(app: &Router, stock: i32) -> i64 {
    let (status, body) = call(app, "POST", "/coupons", None, json!({ "name": "test", "stock": stock })).await;
    assert_eq!(status, 201);
    body["id"].as_i64().unwrap()
}

/// 재고 50 에 200명이 동시에 몰려도 정확히 50명만 성공하고 재고는 0 (음수 X, 남김 X).
async fn race(strategy: &'static str) {
    let app = app().await;
    let (stock, users) = (50, 200);
    let id = create_coupon(&app, stock).await;
    let run = run_id();

    let mut set = tokio::task::JoinSet::new();
    for u in 0..users {
        let app = app.clone();
        set.spawn(async move {
            let uri = format!("/coupons/{id}/reserve?strategy={strategy}");
            call(&app, "POST", &uri, Some(&format!("{run}-{u}")), json!({ "user_id": u })).await.0
        });
    }
    let statuses = set.join_all().await;
    assert_eq!(statuses.iter().filter(|&&s| s == 200).count(), stock as usize, "{strategy}: {statuses:?}");
    assert!(statuses.iter().all(|&s| s == 200 || s == 409), "{strategy}: {statuses:?}");

    let (_, c) = call(&app, "GET", &format!("/coupons/{id}"), None, json!({})).await;
    assert_eq!(c["remaining"], 0);
    assert_eq!(c["reserved"], stock);
    if strategy == "redis" {
        assert_eq!(c["redis_remaining"], 0);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn race_db_lock() {
    race("db_lock").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn race_db_atomic() {
    race("db_atomic").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn race_redis() {
    race("redis").await;
}

/// 같은 유저가 서로 다른 키로 10번 동시에 눌러도 1장만 (중복 요청 방지).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn same_user_gets_one() {
    let app = app().await;
    for strategy in ["db_lock", "db_atomic", "redis"] {
        let id = create_coupon(&app, 10).await;
        let run = run_id();
        let mut set = tokio::task::JoinSet::new();
        for i in 0..10 {
            let app = app.clone();
            set.spawn(async move {
                let uri = format!("/coupons/{id}/reserve?strategy={strategy}");
                call(&app, "POST", &uri, Some(&format!("{run}-{i}")), json!({ "user_id": 1 })).await.0
            });
        }
        let statuses = set.join_all().await;
        assert_eq!(statuses.iter().filter(|&&s| s == 200).count(), 1, "{strategy}: {statuses:?}");
        let (_, c) = call(&app, "GET", &format!("/coupons/{id}"), None, json!({})).await;
        assert_eq!(c["remaining"], 9, "{strategy}");
    }
}

/// 같은 Idempotency-Key 재전송 → 같은 응답, 재고는 한 번만 차감. 다른 요청에 재사용 → 422.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn idempotency_replay() {
    let app = app().await;
    let id = create_coupon(&app, 10).await;
    let key = format!("{}-idem", run_id());
    let uri = format!("/coupons/{id}/reserve");

    let first = call(&app, "POST", &uri, Some(&key), json!({ "user_id": 7 })).await;
    let second = call(&app, "POST", &uri, Some(&key), json!({ "user_id": 7 })).await;
    assert_eq!(first.0, 200);
    assert_eq!(first, second);

    let (_, c) = call(&app, "GET", &format!("/coupons/{id}"), None, json!({})).await;
    assert_eq!(c["remaining"], 9);

    let other = call(&app, "POST", &uri, Some(&key), json!({ "user_id": 8 })).await;
    assert_eq!(other.0, 422);

    let missing = call(&app, "POST", &uri, None, json!({ "user_id": 9 })).await;
    assert_eq!(missing.0, 400);
}
