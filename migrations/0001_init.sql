CREATE TABLE coupons (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT        NOT NULL,
    total_stock INT         NOT NULL CHECK (total_stock >= 0),
    remaining   INT         NOT NULL CHECK (remaining >= 0), -- 최후의 방어선: 음수 재고 불가
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE reservations (
    id              BIGSERIAL PRIMARY KEY,
    coupon_id       BIGINT      NOT NULL REFERENCES coupons(id),
    user_id         BIGINT      NOT NULL,
    idempotency_key TEXT        NOT NULL UNIQUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (coupon_id, user_id) -- 중복 요청 방지: 1인 1매
);
