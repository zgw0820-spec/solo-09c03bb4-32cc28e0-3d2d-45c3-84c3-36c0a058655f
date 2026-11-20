# 巡检工单库交接演示

FastAPI + 双 PostgreSQL 15（逻辑复制）实现两个场景：
1. **单向交接**：旧库（发布端）暂停接单、等在途写入结束、新库（订阅端）收齐
   已提交工单并对齐序列后切换接单。
2. **撞号隔离**：旧库工单在订阅端与新库本地试写件撞号、整笔事务复制卡住时，
   值班员在交接页对照两库原文后确认隔离，保留试写件并让旧库事务继续复制。

## 目录结构

```
.
├── docker-compose.yml            # old-db / new-db / app 三服务
├── app/                          # FastAPI 应用（含 Dockerfile、requirements）
│   └── app/
│       ├── main.py               # 路由与演示网页（工单、交接、隔离、订阅暂停/恢复、日志）
│       ├── cutover.py            # 交接状态机：OLD -> SWITCHING -> NEW，失败/超时回滚
│       ├── quarantine.py         # 撞号隔离业务规则：前置校验、原子搬迁、幂等重放
│       ├── state.py              # 接单库状态持久化（SQLite，重启沿用）
│       ├── db.py                 # PG 访问：工单/试写件、序列快照、复制状态、隔离搬迁
│       └── config.py             # 环境变量配置
├── db/old/init/01_schema.sql     # 旧库：表 + CACHE 5 序列 + 发布
├── db/new/init/                  # 新库：同名表/序列 + 隔离表（不发布）+ 订阅创建脚本
├── scripts/
│   ├── local_pg.sh               # 无 Docker 时用本地 PG 二进制起两库并配置复制
│   ├── local_app.sh              # 无 Docker 时本地启动应用
│   └── local_seed_demo.py        # 灌入撞号演示数据（旧库同事务两张工单，一张撞号）
└── tests/
    ├── e2e_test.py               # 单向交接端到端试跑（29 项断言）
    └── quarantine_test.py        # 撞号隔离端到端试跑（42 项断言，可重复运行）
```

## 方式一：Docker Compose（交付目标）

```bash
docker compose up --build
# 应用 http://localhost:8000 ；旧库 5433、新库 5434

# 灌入撞号演示数据（新库试写件 2000；旧库同事务 2000+2001，2000 撞号）
docker compose exec app python -I /scripts/local_seed_demo.py
# 也可直接点网页“灌入演示数据”按钮（走 /api/demo/seed-collision，效果相同）
```

- `old-db` 以 `wal_level=logical` 启动并创建发布 `inspection_pub`（仅 `work_orders`）；
- `new-db` 启动后带重试创建订阅 `inspection_sub`（`copy_data=true` 初始同步）；
- 隔离表 `work_orders_quarantine` 只建在新库、不加入任何发布，复制流量不触碰它；
- 应用状态保存在 `app-data` 卷，容器重启后沿用已生效的接单库。

## 方式二：本机无 Docker 试跑（本仓库实际验证方式）

当前环境没有 Docker，用解包到用户目录的 PostgreSQL 15 二进制起两库：

```bash
./scripts/local_pg.sh start      # 初始化并启动两库 + 逻辑复制（55433 旧 / 55434 新）
./scripts/local_app.sh           # 启动 FastAPI（http://127.0.0.1:8000，超时阈值 8s）
python -I scripts/local_seed_demo.py   # 灌入撞号演示数据（幂等，可重复执行）
python -I tests/quarantine_test.py     # 撞号隔离端到端试跑（42 项断言）
python -I tests/e2e_test.py            # 单向交接端到端试跑（29 项断言）
./scripts/local_pg.sh status     # 查看两库行数、隔离表行数与订阅状态
./scripts/local_pg.sh stop       # 停止；clean 可清理全部本地数据
```

## 撞号隔离语义与关键设计

1. **场景**：新库先有本地试写件（`source_db='new-trial'`，只在订阅端、不参与
   复制）；旧库一笔事务提交的同号工单回放时主键冲突，逻辑复制 apply worker
   报错并持续重试**整笔事务**——同事务的第二张工单在解除冲突前同样不可见。
2. **交接页对照**：`GET /api/quarantine/compare?work_order_id=` 按工单号返回
   两库原文、试写件版本令牌（整行内容 SHA-256 指纹）与前置条件结论。
3. **隔离前置条件（缺一即 409 拒绝，数据不改）**：
   - 隔离只能在旧库接单期间提交（状态机必须为 `OLD`，SWITCHING/NEW 拒绝）；
   - 旧库须已有同号工单且来源为 `old`；
   - 新库工单表同号行须仍是本地试写件（`new-trial`）且版本令牌与页面所见一致。
4. **原子搬迁**：`POST /api/quarantine/confirm` 在**单个 PG 事务**内完成
   “试写件原样 INSERT 进 `work_orders_quarantine` + 从 `work_orders` DELETE”，
   事务内 `FOR UPDATE` 行锁 + advisory lock 防并发；任一步失败整体回滚。
   隔离表保留工单五列原文，另记隔离时间、确认令牌、撞号来源。
5. **幂等重放**：同工单号已隔离后，重送确认（无论令牌是否相同）始终返回
   **原隔离结果**，不删不盖工单表——即使订阅已把旧库同号工单回填进来。
6. **订阅开关沿用**：隔离操作绝不执行 ENABLE/DISABLE，响应回带操作前后的
   开关状态；启用时由 PostgreSQL 自身重试旧库整笔事务，冲突解除后整笔入库。
7. **网页**：新增“撞号隔离交接”卡片，支持灌演示数据、写试写件、按号对照
   两库原文、确认隔离，并实时展示隔离表中的原件。

## 单向交接语义与关键设计

1. **统一入口**：网页 `POST /api/work-orders` 按当前接单库路由，响应返回工单
   编号与 `accepted_by`（old/new）。
2. **单向交接**：`POST /api/cutover`（需 `confirm=true`）。状态机
   `OLD -> SWITCHING -> NEW`，交接串行，进行中再次发起返回冲突提示。
3. **旧库暂停接单**：进入 SWITCHING 后网页提交返回 503。
4. **等在途写入结束**：进程内在途计数 + 旧库 `pg_stat_activity` 中
   `xact_start/backend_xid` 非空的客户端事务双重确认（覆盖 idle in transaction）。
5. **收齐才接单**：轮询发布端 `pg_current_wal_lsn()` 与
   `pg_stat_replication.replay_lsn` 一致且 `state=streaming`，再核对两库
   行数与最大编号一致。
6. **编号接续（含预留号）**：序列 `CACHE 5`。PostgreSQL 后端预取整块时会把
   块上界写入序列关系，`pg_sequence_last_value()` 返回的即“已预留到的最高号”，
   已包含缓存未用号（短连接预取后断连的块内剩余号）与事务回滚空号。新库
   `setval(seq, 预留最高号, true)`，首个新编号 = 预留最高号 + 1，严格大于旧库
   序列预留的全部号码。
7. **失败/超时回滚**：任一步异常或超过 `CUTOVER_TIMEOUT_SECONDS` 未追平，
   状态回到 OLD，旧库恢复接单，事件日志记录原因。
8. **重启保持与幂等**：状态落 SQLite；NEW 状态下重复点击交接只返回当前接单库，
   不重复切换。
9. **订阅暂停/恢复**：网页按钮执行 `ALTER SUBSCRIPTION DISABLE/ENABLE`。
   暂停期间旧库继续写入可制造复制积压，发起交接即可演示追平超时与自动回滚。

## 已验证结果

### 撞号隔离（tests/quarantine_test.py，42/42 通过，连续两遍均通过）

- 演示数据：旧库同一事务提交 2000、2001，新库先有试写件 2000；订阅端
  回放整笔事务时主键冲突，2001 连带不可见，新库日志可见 `duplicate key` 重试；
- 对照接口返回两库原文与版本令牌；旧库无同号、新库非试写件、令牌错误、
  非 OLD 状态等前置不满足场景全部 409 拒绝且数据不变；
- 注入触发器使搬迁第二步失败时，接口报错且工单表/隔离表都不留痕（回滚验证）；
- 正式隔离后试写件五列原样进隔离表并移开工单表，订阅开关前后一致未被改动；
- PostgreSQL 自动重试后旧库 2000、2001 整笔入库（均为 `old` 来源），
  隔离表原件不受回填影响；
- 重送确认（同令牌/异令牌）均幂等返回原隔离结果，回填的同号工单不删不盖、
  隔离表不重复插入；暂停订阅期间重复确认也不会擅自启用订阅。

### 单向交接（tests/e2e_test.py，29/29 通过）

- 旧库受理工单并自动编号；逻辑复制实时同步到新库；
- 制造回滚空号与缓存未用号后，实际最大发号 < 序列预留最高号，交接计算的
  首个新编号严格大于该预留最高号；
- 暂停订阅 → 交接在 8s 超时 → 504 并自动回滚 OLD，旧库恢复接单；
- 恢复订阅追平后，在途慢事务结束才完成交接；
- 新库序列对齐、新库接单编号接续；重复交接幂等；应用重启后仍为 NEW。

## 已知边界/剩余事项

- 状态机为单应用进程设计（在途计数在进程内）；多副本部署需把在途协调放到
  数据库（如 `pg_advisory_lock`/状态表），本演示未包含。隔离搬迁虽已用
  advisory lock + 行锁保证库内正确性，但多应用实例下的操作审计仍需集中化。
- 撞号隔离仅处理订阅端主键冲突这一类场景；其它复制冲突（如更新缺失行、
  类型不兼容）未覆盖。隔离表没有定期清理/复核流程，需运维侧补充处置 SLA。
- 切换到 NEW 后未实现回切；重复初始化容器时订阅创建脚本已做幂等，但已存在的
  卷上旧数据不会自动清理，重置演示请 `docker compose down -v`。
- 本机无 Docker，compose 仅做了 YAML 与初始化脚本的静态核对，未实际起容器；
  全部 71 项断言在本机 PG 15 双实例上验证通过。
- 生产化还需补充：认证授权、TLS、连接池、监控指标与告警、旧库最终只读/下线流程。
