-- 新库（订阅端）初始化：同名表/序列 + 试写件隔离表（隔离表不发布、不参与复制）
CREATE TABLE IF NOT EXISTS work_orders (
    id          bigint PRIMARY KEY,
    title       text        NOT NULL,
    detail      text        NOT NULL DEFAULT '',
    source_db   text        NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE SEQUENCE IF NOT EXISTS work_orders_id_seq
    AS bigint
    START WITH 1000
    CACHE 5
    OWNED BY work_orders.id;

ALTER TABLE work_orders ALTER COLUMN id
    SET DEFAULT nextval('work_orders_id_seq');

-- 撞号隔离表：保存新库本地试写件的“原样”（列与 work_orders 完全一致，
-- 另加隔离操作审计列）。该表只存在于订阅端，不建在发布端、不加入任何发布，
-- 因此逻辑复制不会触碰它，隔离动作也不会产生复制流量。
CREATE TABLE IF NOT EXISTS work_orders_quarantine (
    id                 bigint PRIMARY KEY,
    title              text        NOT NULL,
    detail             text        NOT NULL DEFAULT '',
    source_db          text        NOT NULL,
    created_at         timestamptz NOT NULL,
    quarantined_at     timestamptz NOT NULL DEFAULT now(),
    -- 隔离确认时页面所见试写件的版本令牌（行内容指纹），
    -- 重送同一次确认时用它判定“仍是同一次确认”
    confirm_token      text        NOT NULL,
    -- 隔离时撞号旧库工单的来源标记，便于审计回放
    collided_source_db text        NOT NULL DEFAULT ''
);

-- 订阅在 shell 脚本中创建（需要重试，等待发布端完全就绪），见 02_create_subscription.sh
