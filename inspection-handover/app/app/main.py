"""FastAPI 入口：工单提交/查询、交接、订阅暂停恢复、撞号隔离，以及演示网页。"""
from fastapi import FastAPI, HTTPException
from fastapi.encoders import jsonable_encoder
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field

from . import db
from .config import get_settings
from .cutover import CutoverError, CutoverManager
from .quarantine import QuarantineError, QuarantineManager
from .state import StateStore

settings = get_settings()
store = StateStore(settings.state_db_path)
manager = CutoverManager(settings, store)
quarantine_manager = QuarantineManager(settings, store)

app = FastAPI(title="巡检工单库交接", version="1.1")


class WorkOrderIn(BaseModel):
    title: str = Field(min_length=1, max_length=200)
    detail: str = Field(default="", max_length=2000)


class CutoverRequest(BaseModel):
    confirm: bool = False


class TrialWorkOrderIn(BaseModel):
    title: str = Field(min_length=1, max_length=200)
    detail: str = Field(default="", max_length=2000)
    explicit_id: int | None = Field(default=None, ge=1)


class CompareRequest(BaseModel):
    work_order_id: int = Field(ge=1)


class ConfirmQuarantineRequest(BaseModel):
    work_order_id: int = Field(ge=1)
    confirm_token: str = Field(min_length=1, max_length=128)


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    return PAGE_HTML


@app.get("/api/status")
def status() -> dict:
    state = manager.current_state()
    accepting = manager.accepting_db()
    sub = manager.subscription_overview()
    return {
        "state": state,
        "accepting_db": accepting,
        "inflight_writes": manager.inflight,
        "subscription": sub,
    }


@app.get("/api/work-orders")
def work_orders() -> dict:
    # 两库数据都展示，source_db 标明每条工单实际由哪个库接单
    return {
        "accepting_db": manager.accepting_db(),
        "old": db.list_work_orders(settings.old_dsn),
        "new": db.list_work_orders(settings.new_dsn),
        "old_summary": {"count_max": db.count_and_max(settings.old_dsn)},
        "new_summary": {"count_max": db.count_and_max(settings.new_dsn)},
    }


@app.post("/api/work-orders")
def submit_work_order(payload: WorkOrderIn) -> dict:
    state = manager.current_state()
    if state == "SWITCHING":
        raise HTTPException(status_code=503, detail="交接进行中，旧库已暂停接单，请稍后再试")
    if state == "NEW":
        target_dsn, target_name = settings.new_dsn, "new"
    else:
        target_dsn, target_name = settings.old_dsn, "old"

    manager.enter_write()
    try:
        row = db.insert_work_order(target_dsn, payload.title, payload.detail, target_name)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"写入失败: {exc}") from exc
    finally:
        manager.exit_write()
    return {"work_order": row, "accepted_by": target_name, "state": state}


@app.post("/api/cutover")
def start_cutover(payload: CutoverRequest) -> JSONResponse:
    if not payload.confirm:
        raise HTTPException(status_code=400, detail="需要 confirm=true 确认发起单向交接")
    try:
        result = manager.start_cutover()
    except CutoverError as exc:
        message = str(exc)
        status_code = 504 if "超时" in message else 500
        # 出错/超时已回滚，返回当前状态（应为 OLD）
        return JSONResponse(
            status_code=status_code,
            content={
                "ok": False,
                "error": message,
                "state": manager.current_state(),
                "accepting_db": manager.accepting_db(),
            },
        )
    return JSONResponse(content={"ok": True, **result})


@app.post("/api/subscription/pause")
def pause_subscription() -> dict:
    try:
        return manager.pause_subscription()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"暂停订阅失败: {exc}") from exc


@app.post("/api/subscription/resume")
def resume_subscription() -> dict:
    try:
        return manager.resume_subscription()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"恢复订阅失败: {exc}") from exc


@app.get("/api/events")
def events() -> list:
    return store.list_events()


# ---- 撞号隔离交接 ------------------------------------------------------
@app.post("/api/trial-work-orders")
def create_trial_work_order(payload: TrialWorkOrderIn) -> dict:
    """在新库写本地试写件。仅旧库接单期间允许（试写是为撞号对照准备的）。"""
    if manager.current_state() != "OLD":
        raise HTTPException(
            status_code=409,
            detail=f"试写只能在旧库接单期间进行（当前状态 {manager.current_state()}）",
        )
    try:
        row = db.insert_trial_work_order(
            settings.new_dsn, payload.title, payload.detail, payload.explicit_id
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"试写失败: {exc}") from exc
    store.log_event(
        "QUARANTINE",
        f"新库本地试写件已写入：工单号 {row['id']}（source_db=new-trial，不参与复制）",
        {"work_order_id": row["id"], "explicit_id": payload.explicit_id},
    )
    return {"trial_work_order": row, "confirm_token": db.row_confirm_token(row)}


@app.get("/api/quarantine/compare")
def quarantine_compare(work_order_id: int) -> dict:
    """交接页按工单号对照两库原文（GET，供页面直接查询）。"""
    try:
        return quarantine_manager.compare(work_order_id)
    except QuarantineError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.message) from exc


@app.post("/api/quarantine/confirm")
def quarantine_confirm(payload: ConfirmQuarantineRequest) -> JSONResponse:
    """确认隔离：试写件原样入隔离表并移出工单表，两步一事务；重复确认幂等。"""
    try:
        result = quarantine_manager.confirm(payload.work_order_id, payload.confirm_token)
    except QuarantineError as exc:
        return JSONResponse(
            status_code=exc.status_code,
            content={"ok": False, "error": exc.message},
        )
    return JSONResponse(content=jsonable_encoder({"ok": True, **result}))


@app.get("/api/quarantine/list")
def quarantine_list() -> dict:
    """网页显示隔离的原件（隔离表只在新库）。"""
    return {"quarantined": jsonable_encoder(db.list_quarantined(settings.new_dsn))}


@app.post("/api/demo/seed-collision")
def demo_seed_collision() -> dict:
    """灌入本地演示数据：新库试写件 2000 + 旧库同一事务提交 2000/2001（2000 撞号）。"""
    if manager.current_state() != "OLD":
        raise HTTPException(
            status_code=409,
            detail=f"演示数据只能在旧库接单期间灌入（当前状态 {manager.current_state()}）",
        )
    try:
        result = db.seed_demo_collision(settings.old_dsn, settings.new_dsn)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"演示数据灌入失败: {exc}") from exc
    store.log_event(
        "QUARANTINE",
        "已灌入撞号演示数据：新库试写件 2000；旧库同一事务提交工单 2000、2001",
        {"skipped": result["skipped"]},
    )
    return result


PAGE_HTML = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<title>巡检工单库交接</title>
<style>
  body { font-family: system-ui, "PingFang SC", "Microsoft YaHei", sans-serif; margin: 24px; background: #f6f7f9; color: #1f2933; }
  h1 { font-size: 20px; } h2 { font-size: 16px; margin-top: 28px; }
  .card { background: #fff; border: 1px solid #e4e7eb; border-radius: 8px; padding: 16px; margin-bottom: 16px; }
  table { border-collapse: collapse; width: 100%; font-size: 13px; }
  th, td { border: 1px solid #e4e7eb; padding: 6px 8px; text-align: left; }
  th { background: #f0f2f5; }
  button { padding: 8px 14px; border: 0; border-radius: 6px; cursor: pointer; margin-right: 8px; }
  .primary { background: #2563eb; color: #fff; } .warn { background: #d97706; color: #fff; }
  .ok { background: #059669; color: #fff; } .muted { background: #e4e7eb; color: #334155; }
  .badge { display: inline-block; padding: 2px 10px; border-radius: 999px; font-size: 12px; font-weight: 600; }
  .b-old { background: #dbeafe; color: #1d4ed8; } .b-new { background: #d1fae5; color: #047857; }
  .b-switching { background: #fef3c7; color: #b45309; }
  input, textarea { width: 100%; padding: 8px; margin: 4px 0 10px; border: 1px solid #cbd2d9; border-radius: 6px; box-sizing: border-box; }
  pre { background: #0f172a; color: #e2e8f0; padding: 12px; border-radius: 8px; font-size: 12px; overflow:auto; max-height: 260px; }
  .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
  #msg { margin: 8px 0; font-size: 13px; white-space: pre-wrap; }
</style>
</head>
<body>
<h1>巡检工单库交接演示（PostgreSQL 逻辑复制）</h1>

<div class="card">
  <strong>当前状态：</strong><span id="state" class="badge b-old">-</span>
  &nbsp;接单库：<span id="accepting" class="badge b-old">-</span>
  &nbsp;在途写入：<span id="inflight">-</span>
  &nbsp;订阅：<span id="substate">-</span>
  <div style="margin-top:10px">
    <button class="muted" onclick="refresh()">刷新状态</button>
    <button class="primary" onclick="startCutover()">发起单向交接（旧库 → 新库）</button>
    <button class="warn" onclick="pauseSub()">暂停订阅（制造追平延迟）</button>
    <button class="ok" onclick="resumeSub()">恢复订阅</button>
  </div>
  <div id="msg"></div>
</div>

<div class="card">
  <h2 style="margin-top:0">提交巡检工单（统一入口，自动发往当前接单库）</h2>
  <label>标题</label><input id="title" placeholder="例如：3号机组例行巡检">
  <label>详情</label><textarea id="detail" rows="2"></textarea>
  <button class="primary" onclick="submitOrder()">提交工单</button>
  <div id="submitmsg"></div>
</div>

<div class="grid">
  <div class="card">
    <h2 style="margin-top:0">旧库工单（发布端） <small id="oldsum"></small></h2>
    <table><thead><tr><th>编号</th><th>标题</th><th>来源</th><th>时间</th></tr></thead>
    <tbody id="oldrows"></tbody></table>
  </div>
  <div class="card">
    <h2 style="margin-top:0">新库工单（订阅端） <small id="newsum"></small></h2>
    <table><thead><tr><th>编号</th><th>标题</th><th>来源</th><th>时间</th></tr></thead>
    <tbody id="newrows"></tbody></table>
  </div>
</div>

<div class="card" style="border-color:#f59e0b;">
  <h2 style="margin-top:0">撞号隔离交接（仅旧库接单期间可提交）</h2>
  <div style="margin-bottom:10px">
    <button class="warn" onclick="seedDemo()">灌入演示数据（新库试写件 2000；旧库同事务 2000+2001，2000 撞号）</button>
    <button class="muted" onclick="createTrial()">新库写本地试写件（序列发号）</button>
  </div>
  <label>按工单号对照两库原文</label>
  <div style="display:flex; gap:8px;">
    <input id="cmpid" style="margin:0" placeholder="例如：2000" value="2000">
    <button class="primary" style="white-space:nowrap" onclick="compareOrder()">对照</button>
  </div>
  <div id="cmpmsg"></div>
  <div class="grid" style="margin-top:10px">
    <div>
      <strong>旧库原文（发布端，须已有同号工单）</strong>
      <pre id="oldcmp" style="max-height:180px">（未对照）</pre>
    </div>
    <div>
      <strong>新库原文（订阅端，须仍是本地试写版本）</strong>
      <pre id="newcmp" style="max-height:180px">（未对照）</pre>
    </div>
  </div>
  <div style="margin-top:8px">
    <button class="ok" onclick="confirmQuarantine()">确认隔离（原样入隔离表并移出工单表，一事务提交）</button>
    <span id="tokinfo" style="font-size:12px;color:#64748b"></span>
  </div>
  <div id="qmsg"></div>
  <h2 style="margin-top:18px">隔离表原件（新库 work_orders_quarantine）</h2>
  <table><thead><tr><th>编号</th><th>标题</th><th>来源</th><th>试写时间</th><th>隔离时间</th><th>撞号来源</th></tr></thead>
  <tbody id="qrows"></tbody></table>
</div>

<div class="card">
  <h2 style="margin-top:0">交接事件日志</h2>
  <pre id="events"></pre>
</div>

<script>
function badgeClass(v){ return v === 'NEW' ? 'b-new' : v === 'SWITCHING' ? 'b-switching' : 'b-old'; }
function show(el, text, color){ el.textContent = text; el.style.color = color || '#334155'; }

async function refresh(){
  const s = await (await fetch('/api/status')).json();
  const st = document.getElementById('state');
  st.textContent = s.state; st.className = 'badge ' + badgeClass(s.state);
  const ac = document.getElementById('accepting');
  ac.textContent = s.accepting_db; ac.className = 'badge ' + badgeClass(s.accepting_db === 'new' ? 'NEW' : 'OLD');
  document.getElementById('inflight').textContent = s.inflight_writes;
  const sub = s.subscription.subscriber;
  document.getElementById('substate').textContent =
    sub.exists ? (sub.enabled ? '启用中' : '已暂停') : '不存在';

  const w = await (await fetch('/api/work-orders')).json();
  fillRows('oldrows', w.old); fillRows('newrows', w.new);
  document.getElementById('oldsum').textContent = '行数/最大号: ' + w.old_summary.count_max.join(' / ');
  document.getElementById('newsum').textContent = '行数/最大号: ' + w.new_summary.count_max.join(' / ');
  loadQuarantine();
  const ev = await (await fetch('/api/events')).json();
  document.getElementById('events').textContent =
    ev.map(e => new Date(e.ts*1000).toLocaleTimeString() + ' [' + e.phase + '] ' + e.message).join('\\n') || '（暂无）';
}

function fillRows(id, rows){
  document.getElementById(id).innerHTML = rows.map(r =>
    '<tr><td>' + r.id + '</td><td>' + escapeHtml(r.title) + '</td><td>' + r.source_db +
    '</td><td>' + new Date(r.created_at).toLocaleString() + '</td></tr>').join('') ||
    '<tr><td colspan="4">（无）</td></tr>';
}
function escapeHtml(s){ return (s||'').replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c])); }

async function submitOrder(){
  const body = { title: document.getElementById('title').value.trim(), detail: document.getElementById('detail').value };
  if(!body.title){ show(document.getElementById('submitmsg'), '请填写标题', '#b91c1c'); return; }
  const r = await fetch('/api/work-orders', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body) });
  const d = await r.json();
  if(r.ok){
    show(document.getElementById('submitmsg'),
      '已受理：编号 ' + d.work_order.id + '，接单库：' + d.accepted_by, '#047857');
    document.getElementById('title').value = ''; document.getElementById('detail').value = '';
    refresh();
  } else {
    show(document.getElementById('submitmsg'), '提交失败：' + (d.detail || r.status), '#b91c1c');
  }
}

async function startCutover(){
  if(!confirm('确认发起单向交接？旧库将暂停接单，追平并对齐序列后由新库接单。')) return;
  const r = await fetch('/api/cutover', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({confirm:true}) });
  const d = await r.json();
  const el = document.getElementById('msg');
  if(r.ok && d.ok){
    show(el, d.already_done ? '交接早已生效，当前接单库：新库（重复点击不重复切换）'
      : '交接成功！首个新编号：' + d.first_new_id + '，旧库最高预留号：' + d.old_highest_reserved, '#047857');
  } else {
    show(el, '交接失败已回滚，旧库恢复接单：' + d.error, '#b91c1c');
  }
  refresh();
}

async function pauseSub(){
  const r = await fetch('/api/subscription/pause', { method:'POST' });
  show(document.getElementById('msg'), r.ok ? '订阅已暂停，可继续在旧库写工单制造延迟，再发起交接观察超时' : '操作失败', r.ok ? '#b45309' : '#b91c1c');
  refresh();
}
async function resumeSub(){
  const r = await fetch('/api/subscription/resume', { method:'POST' });
  show(document.getElementById('msg'), r.ok ? '订阅已恢复，积压数据将开始追平' : '操作失败', r.ok ? '#047857' : '#b91c1c');
  refresh();
}

// ---- 撞号隔离 ----
let currentToken = null;
function pretty(obj){ return obj ? JSON.stringify(obj, null, 2) : '（无记录）'; }

async function seedDemo(){
  const r = await fetch('/api/demo/seed-collision', { method:'POST' });
  const d = await r.json();
  show(document.getElementById('qmsg'), r.ok
    ? '演示数据已就绪（重复灌入会跳过已存在部分）：' + JSON.stringify(d.skipped.length ? d.skipped : '全部新建')
    : '灌入失败：' + (d.detail || r.status), r.ok ? '#047857' : '#b91c1c');
  refresh();
}

async function createTrial(){
  const r = await fetch('/api/trial-work-orders', {
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({ title: '值班员新库试写件 ' + new Date().toLocaleTimeString(), detail: '本地试写，不参与逻辑复制' })
  });
  const d = await r.json();
  show(document.getElementById('qmsg'), r.ok
    ? '试写件已写入新库，工单号 ' + d.trial_work_order.id + '（source_db=new-trial）'
    : '试写失败：' + (d.detail || r.status), r.ok ? '#047857' : '#b91c1c');
  refresh();
}

async function compareOrder(){
  const id = document.getElementById('cmpid').value.trim();
  if(!id){ show(document.getElementById('cmpmsg'), '请输入工单号', '#b91c1c'); return; }
  const r = await fetch('/api/quarantine/compare?work_order_id=' + encodeURIComponent(id));
  const d = await r.json();
  if(!r.ok){ show(document.getElementById('cmpmsg'), '对照失败：' + (d.detail || r.status), '#b91c1c'); return; }
  document.getElementById('oldcmp').textContent = pretty(d.old);
  document.getElementById('newcmp').textContent = d.already_quarantined
    ? '该工单号已隔离，原件见下方隔离表（重复确认将返回原隔离结果）' : pretty(d.new_trial);
  currentToken = d.confirm_token;
  document.getElementById('tokinfo').textContent =
    '版本令牌：' + (currentToken ? currentToken.slice(0, 16) + '…' : '无');
  if(d.already_quarantined){
    show(document.getElementById('cmpmsg'), '该工单号已隔离，重复确认幂等返回原结果', '#047857');
  } else if(d.eligible){
    show(document.getElementById('cmpmsg'), '前置条件满足：旧库有同号工单，新库仍是本地试写件，可确认隔离', '#047857');
  } else {
    show(document.getElementById('cmpmsg'), '不满足隔离条件：' + d.reasons.join('；'), '#b91c1c');
  }
}

async function confirmQuarantine(){
  const id = document.getElementById('cmpid').value.trim();
  if(!id){ show(document.getElementById('qmsg'), '请先输入并对照工单号', '#b91c1c'); return; }
  // 未对照过则先对照取令牌
  if(!currentToken){ await compareOrder(); }
  if(!currentToken){ show(document.getElementById('qmsg'), '没有可用的版本令牌，请先对照', '#b91c1c'); return; }
  if(!confirm('确认把工单号 ' + id + ' 的新库试写件原样隔离并移出工单表？')) return;
  const r = await fetch('/api/quarantine/confirm', {
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({ work_order_id: Number(id), confirm_token: currentToken })
  });
  const d = await r.json();
  if(r.ok && d.ok){
    show(document.getElementById('qmsg'),
      (d.idempotent ? '重复确认：返回原隔离结果（幂等，未改动工单表）' : '隔离成功，两步已在同一事务提交')
      + '；订阅开关：' + (d.subscription_enabled_after ? '启用（PostgreSQL 将自动重试旧库整笔事务）' : '暂停'),
      '#047857');
    currentToken = null;
  } else {
    show(document.getElementById('qmsg'), '隔离被拒绝（已回滚，数据未改动）：' + (d.error || r.status), '#b91c1c');
  }
  refresh();
}

async function loadQuarantine(){
  const d = await (await fetch('/api/quarantine/list')).json();
  document.getElementById('qrows').innerHTML = (d.quarantined || []).map(r =>
    '<tr><td>' + r.id + '</td><td>' + escapeHtml(r.title) + '</td><td>' + r.source_db +
    '</td><td>' + new Date(r.created_at).toLocaleString() +
    '</td><td>' + new Date(r.quarantined_at).toLocaleString() +
    '</td><td>' + escapeHtml(r.collided_source_db) + '</td></tr>').join('')
    || '<tr><td colspan="6">（暂无隔离件）</td></tr>';
}

refresh(); setInterval(refresh, 4000);
</script>
</body>
</html>
"""
