
import hashlib, secrets, sqlite3
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DB_PATH = DATA / "app.db"
FRONTEND = ROOT / "frontend"
SESSIONS: dict[str, str] = {}

# 库路径经函数间接读取：测试可稳定替换 get_db_path 的返回值，
# 避免直接改模块全局变量时被夹具撤销时序影响。
_db_path_override: Path | None = None


def get_db_path() -> Path:
    return _db_path_override if _db_path_override is not None else DB_PATH

# ---------------------------------------------------------------------------
# 全局正确性口径（唯一，禁止分叉）：甲路径——幂等跳过（idempotent_skip）。
# 目标新键已在正式区则跳过写入，只记 replay_skip 审计；任何情况下不得覆盖
# 已正式行。乙路径（修订覆盖 + generation 抬升 + 被盖快照）本仓明确不采用。
# ---------------------------------------------------------------------------
REPLAY_SEMANTICS = "idempotent_skip"

OUTCOME_APPLIED = "applied"              # 首写正式区成功
OUTCOME_SKIP = "replay_skip"             # 幂等跳过：正式键已存在，只审计
OUTCOME_CONTENTION = "replay_contention"  # 后到争用：冲突已关闭，只留痕
OUTCOME_NO_MAPPING = "no_mapping"        # 重放时仍无映射，业务性未成
TERMINAL_OUTCOMES = (OUTCOME_APPLIED, OUTCOME_SKIP)
KNOWN_OUTCOMES = TERMINAL_OUTCOMES + (OUTCOME_CONTENTION, OUTCOME_NO_MAPPING)

CONFLICT_PENDING = "挂起"
CONFLICT_CLOSED = "已关闭"


class ReplayConflict(Exception):
    """重放业务性未成（争用/无映射等）。回执为顶层 JSON，不套 detail 壳。"""

    def __init__(self, status_code: int, receipt: dict):
        self.status_code = status_code
        self.receipt = receipt
        super().__init__(receipt.get("detail", "replay conflict"))


def connect():
    DATA.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(get_db_path(), timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    # 并发重放时让后到者等待写锁，而不是立刻报 database is locked
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def sha(p: str) -> str:
    return hashlib.sha256(p.encode("utf-8")).hexdigest()


def ensure_users(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS users(
        username TEXT PRIMARY KEY, password_sha TEXT NOT NULL, role TEXT NOT NULL)""")
    if conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"] == 0:
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("admin", sha("123456"), "管理员"))
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("viewer", sha("view123"), "只读"))


def _columns(conn, table: str) -> set[str]:
    return {r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _migrate(conn):
    """旧库补列/补索引；全新库建表已含全部结构，此处全部跳过。"""
    formal_cols = _columns(conn, "formal_entities")
    if formal_cols and "generation" not in formal_cols:
        conn.execute("ALTER TABLE formal_entities ADD COLUMN generation INTEGER NOT NULL DEFAULT 1")
    conf_cols = _columns(conn, "conflicts")
    for col, ddl in (
        ("target_new_key", "ALTER TABLE conflicts ADD COLUMN target_new_key TEXT"),
        ("version", "ALTER TABLE conflicts ADD COLUMN version TEXT"),
        ("closed_at", "ALTER TABLE conflicts ADD COLUMN closed_at TEXT"),
        ("closed_replay_id", "ALTER TABLE conflicts ADD COLUMN closed_replay_id INTEGER"),
    ):
        if conf_cols and col not in conf_cols:
            conn.execute(ddl)
    # 历史脏数据若有重复正式键，保留最早一行，再建唯一索引兜底幂等口径
    dupes = conn.execute(
        "SELECT entity_key FROM formal_entities GROUP BY entity_key HAVING COUNT(*) > 1"
    ).fetchall()
    for d in dupes:
        keep = conn.execute(
            "SELECT MIN(id) m FROM formal_entities WHERE entity_key=?", (d["entity_key"],)
        ).fetchone()["m"]
        conn.execute("DELETE FROM formal_entities WHERE entity_key=? AND id<>?",
                     (d["entity_key"], keep))
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_formal_entity_key ON formal_entities(entity_key)")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_replays_conflict ON conflict_replays(conflict_id)")


class LoginIn(BaseModel):
    username: str
    password: str


def require_user(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "未登录")
    u = SESSIONS.get(authorization.removeprefix("Bearer ").strip())
    if not u:
        raise HTTPException(401, "登录已失效")
    return u


PRODUCT = "实体键桥"
SKELETON = "实体键映射生效后入站先重映再进正式区，冲突进队列，映射版本与正式区对账"

app = FastAPI(title=PRODUCT)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True,
                   allow_methods=["*"], allow_headers=["*"])


@app.exception_handler(ReplayConflict)
def _replay_conflict_handler(_req: Request, exc: ReplayConflict):
    return JSONResponse(status_code=exc.status_code, content=exc.receipt)


def init_db():
    with connect() as conn:
        ensure_users(conn)
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS map_versions(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          version TEXT UNIQUE NOT NULL,
          status TEXT NOT NULL,
          effective_from TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS mappings(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          version TEXT NOT NULL,
          old_key TEXT NOT NULL,
          new_key TEXT NOT NULL,
          kind TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS formal_entities(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          entity_key TEXT NOT NULL,
          mapped_from TEXT NOT NULL,
          version TEXT NOT NULL,
          generation INTEGER NOT NULL DEFAULT 1,
          created_at TEXT NOT NULL,
          UNIQUE(entity_key)
        );
        CREATE TABLE IF NOT EXISTS conflicts(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          old_key TEXT NOT NULL,
          reason TEXT NOT NULL,
          status TEXT NOT NULL,
          target_new_key TEXT,
          version TEXT,
          closed_at TEXT,
          closed_replay_id INTEGER
        );
        CREATE TABLE IF NOT EXISTS conflict_replays(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          conflict_id INTEGER NOT NULL,
          replay_at TEXT NOT NULL,
          outcome TEXT NOT NULL,
          generation INTEGER NOT NULL,
          formal_id INTEGER,
          entity_key TEXT,
          mapped_from TEXT,
          version TEXT,
          detail TEXT NOT NULL,
          FOREIGN KEY(conflict_id) REFERENCES conflicts(id)
        );
        """)
        _migrate(conn)
        if conn.execute("SELECT COUNT(*) c FROM map_versions").fetchone()["c"] == 0:
            conn.execute("INSERT INTO map_versions(version,status,effective_from) VALUES (?,?,?)",
                         ("m-2026.11", "生效", "2026-11-01"))
            conn.executemany(
                "INSERT INTO mappings(version,old_key,new_key,kind) VALUES (?,?,?,?)", [
                    ("m-2026.11", "OLD-A1", "NEW-100", "一对一"),
                    ("m-2026.11", "OLD-B1", "NEW-200", "多对一"),
                    ("m-2026.11", "OLD-B2", "NEW-200", "多对一"),
                ])
            conn.execute(
                "INSERT INTO formal_entities(entity_key,mapped_from,version,generation,created_at)"
                " VALUES (?,?,?,1,?)",
                ("NEW-100", "OLD-A1", "m-2026.11", "2026-11-10T09:00:00"))
            conn.execute("INSERT INTO conflicts(old_key,reason,status) VALUES (?,?,?)",
                         ("OLD-C9", "无映射", CONFLICT_PENDING))


@app.on_event("startup")
def _s():
    init_db()


@app.get("/api/health")
def health():
    with connect() as c:
        n = c.execute("SELECT COUNT(*) c FROM mappings").fetchone()["c"]
    return {"ok": True, "product": PRODUCT, "skeleton": SKELETON,
            "mappings": n, "db": str(get_db_path()), "replay_semantics": REPLAY_SEMANTICS}


@app.post("/api/login")
def login(body: LoginIn):
    with connect() as c:
        row = c.execute(
            "SELECT username,role FROM users WHERE username=? AND password_sha=?",
            (body.username, sha(body.password))).fetchone()
    if not row:
        raise HTTPException(401, "账号或密码错误")
    tok = secrets.token_hex(16)
    SESSIONS[tok] = row["username"]
    return {"token": tok, "username": row["username"], "role": row["role"]}


@app.get("/api/map-versions")
def versions(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items": [dict(r) for r in c.execute(
            "SELECT * FROM map_versions ORDER BY id").fetchall()]}


@app.get("/api/mappings")
def mappings(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items": [dict(r) for r in c.execute(
            "SELECT * FROM mappings ORDER BY id").fetchall()]}


class RemapIn(BaseModel):
    old_key: str


@app.post("/api/remap")
def remap(body: RemapIn, authorization: str | None = Header(default=None)):
    """入站重映。甲路径口径：正式键唯一，重复入站幂等跳过，绝不插第二条正式。"""
    require_user(authorization)
    now = datetime.now().isoformat(timespec="seconds")
    with connect() as c:
        ver = c.execute(
            "SELECT version FROM map_versions WHERE status='生效' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if not ver:
            raise HTTPException(404, "无生效映射版本")
        m = c.execute(
            "SELECT * FROM mappings WHERE version=? AND old_key=?",
            (ver["version"], body.old_key)).fetchone()
        if not m:
            c.execute("INSERT INTO conflicts(old_key,reason,status) VALUES (?,?,?)",
                      (body.old_key, "无映射", CONFLICT_PENDING))
            c.commit()
            raise HTTPException(409, "无映射，已进冲突队列")
        existing = c.execute(
            "SELECT * FROM formal_entities WHERE entity_key=?", (m["new_key"],)).fetchone()
        if existing:
            return {"entity_key": m["new_key"], "version": ver["version"],
                    "action": "idempotent_skip", "formal_id": existing["id"],
                    "generation": existing["generation"]}
        try:
            cur = c.execute(
                "INSERT INTO formal_entities(entity_key,mapped_from,version,generation,created_at)"
                " VALUES (?,?,?,1,?)",
                (m["new_key"], body.old_key, ver["version"], now))
        except sqlite3.IntegrityError:
            # 唯一索引兜底：并发下别人先写了同一正式键，重读走幂等
            row = c.execute(
                "SELECT * FROM formal_entities WHERE entity_key=?", (m["new_key"],)).fetchone()
            return {"entity_key": m["new_key"], "version": ver["version"],
                    "action": "idempotent_skip", "formal_id": row["id"],
                    "generation": row["generation"]}
        return {"entity_key": m["new_key"], "version": ver["version"],
                "action": "applied", "formal_id": cur.lastrowid, "generation": 1}


@app.get("/api/formal")
def formal(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items": [dict(r) for r in c.execute(
            "SELECT * FROM formal_entities ORDER BY id DESC").fetchall()]}


@app.get("/api/conflicts")
def conflicts(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items": [dict(r) for r in c.execute(
            "SELECT * FROM conflicts ORDER BY id DESC").fetchall()]}


def _register_replay(conn, conflict_id, now, outcome, generation,
                     formal_id, entity_key, mapped_from, version, detail) -> int:
    """重放登记表只追加；四要素 conflict_id/replay_at/outcome/generation 齐备。"""
    cur = conn.execute(
        """INSERT INTO conflict_replays
           (conflict_id,replay_at,outcome,generation,formal_id,entity_key,
            mapped_from,version,detail)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        (conflict_id, now, outcome, generation, formal_id, entity_key,
         mapped_from, version, detail))
    return cur.lastrowid


def _receipt(conflict_id, replay_id, now, outcome, generation, formal_id,
             entity_key, detail, conflict_status, version=None) -> dict:
    return {
        "semantics": REPLAY_SEMANTICS,
        "conflict_id": conflict_id,
        "replay_id": replay_id,
        "replay_at": now,
        "outcome": outcome,
        "generation": generation,
        "formal_id": formal_id,
        "entity_key": entity_key,
        "version": version,
        "conflict_status": conflict_status,
        "detail": detail,
    }


def run_conflict_replay(conflict_id: int) -> dict:
    """
    冲突重放（甲路径，全局唯一口径）。
    单事务内完成：状态判定 -> 正式区幂等校验 -> 登记 -> 状态关闭。
    任何异常整体回滚：正式区、登记审计、冲突状态同生共死。
    """
    now = datetime.now().isoformat(timespec="seconds")
    conn = connect()
    try:
        # BEGIN IMMEDIATE 立即取 RESERVED 写锁：同一 conflict_id 的并发重放
        # 在 SQLite 层串行化，杜绝无锁双写正式。
        conn.execute("BEGIN IMMEDIATE")
        conflict = conn.execute(
            "SELECT * FROM conflicts WHERE id=?", (conflict_id,)).fetchone()
        if conflict is None:
            conn.rollback()
            raise HTTPException(404, "冲突不存在")

        # 已关闭：后到者只留争用登记，不动正式、不动状态
        if conflict["status"] != CONFLICT_PENDING:
            gen, fid = 0, None
            if conflict["target_new_key"]:
                f = conn.execute(
                    "SELECT * FROM formal_entities WHERE entity_key=?",
                    (conflict["target_new_key"],)).fetchone()
                if f:
                    gen, fid = f["generation"], f["id"]
            rid = _register_replay(
                conn, conflict_id, now, OUTCOME_CONTENTION, gen, fid,
                conflict["target_new_key"], conflict["old_key"], conflict["version"],
                "冲突已关闭，后到重放仅登记争用，未写正式区")
            conn.commit()
            raise ReplayConflict(409, _receipt(
                conflict_id, rid, now, OUTCOME_CONTENTION, gen, fid,
                conflict["target_new_key"], "冲突已关闭，后到重放仅留争用",
                conflict["status"], conflict["version"]))

        ver = conn.execute(
            "SELECT version FROM map_versions WHERE status='生效' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        version = ver["version"] if ver else None
        m = conn.execute(
            "SELECT * FROM mappings WHERE version=? AND old_key=?",
            (version, conflict["old_key"])).fetchone() if ver else None

        # 业务性未成：仍无映射。登记后提交，冲突保持挂起（这不是系统异常，不回滚）
        if not m:
            rid = _register_replay(
                conn, conflict_id, now, OUTCOME_NO_MAPPING, 0, None, None,
                conflict["old_key"], version, "重放时仍无生效映射，冲突保持挂起")
            conn.commit()
            raise ReplayConflict(409, _receipt(
                conflict_id, rid, now, OUTCOME_NO_MAPPING, 0, None, None,
                "重放时仍无生效映射", CONFLICT_PENDING, version))

        target = m["new_key"]
        formal = conn.execute(
            "SELECT * FROM formal_entities WHERE entity_key=?", (target,)).fetchone()

        if formal:
            # 幂等跳过：不写正式，只记 replay_skip 审计，再关闭冲突
            rid = _register_replay(
                conn, conflict_id, now, OUTCOME_SKIP, formal["generation"], formal["id"],
                target, conflict["old_key"], version,
                "目标正式键已存在，跳过写入，仅登记 replay_skip")
            conn.execute(
                "UPDATE conflicts SET status=?,target_new_key=?,version=?,"
                "closed_at=?,closed_replay_id=? WHERE id=?",
                (CONFLICT_CLOSED, target, version, now, rid, conflict_id))
            conn.commit()
            return _receipt(
                conflict_id, rid, now, OUTCOME_SKIP, formal["generation"], formal["id"],
                target, "目标正式键已存在，已跳过写入", CONFLICT_CLOSED, version)

        # 目标键不存在：首写正式区（generation=1），登记 applied，关闭冲突
        cur = conn.execute(
            "INSERT INTO formal_entities(entity_key,mapped_from,version,generation,created_at)"
            " VALUES (?,?,?,1,?)",
            (target, conflict["old_key"], version, now))
        formal_id = cur.lastrowid
        rid = _register_replay(
            conn, conflict_id, now, OUTCOME_APPLIED, 1, formal_id, target,
            conflict["old_key"], version, "首写正式区成功，冲突关闭")
        conn.execute(
            "UPDATE conflicts SET status=?,target_new_key=?,version=?,"
            "closed_at=?,closed_replay_id=? WHERE id=?",
            (CONFLICT_CLOSED, target, version, now, rid, conflict_id))
        conn.commit()
        return _receipt(
            conflict_id, rid, now, OUTCOME_APPLIED, 1, formal_id, target,
            "已写入正式区并关闭冲突", CONFLICT_CLOSED, version)
    except ReplayConflict:
        raise
    except HTTPException:
        # 404 等请求类异常：先回滚再上抛（404 路径已自行 rollback，重复调用无害）
        conn.rollback()
        raise
    except Exception:
        # 失败闭：正式写入、审计登记、状态更新同事务回滚，不留半句谎话
        conn.rollback()
        raise HTTPException(500, "重放失败，正式区与重放审计已同事务回滚")
    finally:
        conn.close()


@app.post("/api/conflicts/{conflict_id}/replay")
def replay_conflict(conflict_id: int,
                    authorization: str | None = Header(default=None)):
    require_user(authorization)
    return run_conflict_replay(conflict_id)


@app.get("/api/conflict-replays")
def replay_audit(conflict_id: int | None = None,
                 authorization: str | None = Header(default=None)):
    """重放登记/审计流水（只读），可按 conflict_id 过滤。"""
    require_user(authorization)
    with connect() as c:
        if conflict_id is None:
            rows = c.execute(
                "SELECT * FROM conflict_replays ORDER BY id DESC").fetchall()
        else:
            rows = c.execute(
                "SELECT * FROM conflict_replays WHERE conflict_id=? ORDER BY id DESC",
                (conflict_id,)).fetchall()
        return {"semantics": REPLAY_SEMANTICS, "items": [dict(r) for r in rows]}


@app.get("/api/generations")
def generations(authorization: str | None = Header(default=None)):
    """互证世代列表（只读）：每个正式键一行，附被重放登记引用次数。"""
    require_user(authorization)
    with connect() as c:
        rows = c.execute(
            """SELECT f.id, f.entity_key, f.mapped_from, f.version, f.generation,
                      f.created_at,
                      (SELECT COUNT(*) FROM conflict_replays r
                        WHERE r.formal_id=f.id) AS replay_refs
               FROM formal_entities f ORDER BY f.id""").fetchall()
        return {"semantics": REPLAY_SEMANTICS, "items": [dict(r) for r in rows]}


def build_verification(conn, conflict_id: int) -> dict:
    """互证卡：冲突区、正式区、重放审计、世代列表五方对质，全部只读。"""
    conflict = conn.execute(
        "SELECT * FROM conflicts WHERE id=?", (conflict_id,)).fetchone()
    if conflict is None:
        raise HTTPException(404, "冲突不存在")
    replays = conn.execute(
        "SELECT * FROM conflict_replays WHERE conflict_id=? ORDER BY id",
        (conflict_id,)).fetchall()
    target_key = conflict["target_new_key"]
    formal = conn.execute(
        "SELECT * FROM formal_entities WHERE entity_key=?", (target_key,)
    ).fetchone() if target_key else None

    terminal = [r for r in replays if r["outcome"] in TERMINAL_OUTCOMES]
    contentions = [r for r in replays if r["outcome"] == OUTCOME_CONTENTION]
    checks = []

    # 1 登记闭合：关闭冲突恰有一条终局登记且 closed_replay_id 指向它
    if conflict["status"] == CONFLICT_CLOSED:
        ok = len(terminal) == 1 and conflict["closed_replay_id"] == terminal[0]["id"]
        detail = f"终局登记 {len(terminal)} 条，closed_replay_id={conflict['closed_replay_id']}"
    else:
        ok = len(terminal) == 0
        detail = f"挂起冲突不得有终局登记，当前 {len(terminal)} 条"
    checks.append({"name": "登记闭合", "ok": ok, "detail": detail})

    # 2 世代一致：每条登记的 generation 必须与正式区当前世代互证
    gen_bad = []
    for r in replays:
        if not r["entity_key"]:
            continue
        f = conn.execute(
            "SELECT * FROM formal_entities WHERE entity_key=?", (r["entity_key"],)).fetchone()
        if f is None:
            gen_bad.append(f"登记#{r['id']} 目标正式行缺失")
        elif f["generation"] != r["generation"]:
            gen_bad.append(f"登记#{r['id']} 世代{r['generation']}≠正式{f['generation']}")
    checks.append({
        "name": "世代一致", "ok": not gen_bad,
        "detail": "；".join(gen_bad) or "全部登记世代与正式区互证一致"})

    # 3 正式唯一：幂等架构下同一 entity_key 不得有第二行
    cnt = conn.execute(
        "SELECT COUNT(*) t, COUNT(DISTINCT entity_key) d FROM formal_entities"
    ).fetchone()
    checks.append({
        "name": "正式唯一", "ok": cnt["t"] == cnt["d"],
        "detail": f"正式行 {cnt['t']} / 去重正式键 {cnt['d']}（UNIQUE 索引兜底）"})

    # 4 无无痕覆盖：每条终局登记都必须能回溯到现存正式行
    shadow = []
    for r in terminal:
        f = conn.execute(
            "SELECT * FROM formal_entities WHERE id=?", (r["formal_id"],)).fetchone()
        if f is None:
            shadow.append(f"终局登记#{r['id']} 回溯正式行失败")
    checks.append({
        "name": "无无痕覆盖", "ok": not shadow,
        "detail": "；".join(shadow) or "每条终局登记均可回溯正式行，不存在无审计覆盖"})

    # 5 争用留痕：后到重放只能是 contention，且争用只可能发生在关闭后
    unknown = [r for r in replays if r["outcome"] not in KNOWN_OUTCOMES]
    contention_ok = (not unknown and
                     (not contentions or conflict["status"] == CONFLICT_CLOSED))
    checks.append({
        "name": "争用留痕", "ok": contention_ok,
        "detail": f"争用登记 {len(contentions)} 条，后到者只留痕不写正式"})

    generation_list = [dict(r) for r in conn.execute(
        """SELECT f.id, f.entity_key, f.generation, f.version,
                  (SELECT COUNT(*) FROM conflict_replays r WHERE r.formal_id=f.id) AS replay_refs
           FROM formal_entities f ORDER BY f.id""").fetchall()]

    return {
        "semantics": REPLAY_SEMANTICS,
        "conflict": dict(conflict),
        "formal": dict(formal) if formal else None,
        "replays": [dict(r) for r in replays],
        "generation_list": generation_list,
        "checks": checks,
        "all_ok": all(x["ok"] for x in checks),
    }


@app.get("/api/conflicts/{conflict_id}/verification")
def verification(conflict_id: int,
                 authorization: str | None = Header(default=None)):
    """重放结局核证只读接口：互证卡。"""
    require_user(authorization)
    with connect() as c:
        return build_verification(c, conflict_id)


@app.get("/")
def index():
    return FileResponse(FRONTEND / "index.html")


app.mount("/static", StaticFiles(directory=str(FRONTEND)), name="static")
