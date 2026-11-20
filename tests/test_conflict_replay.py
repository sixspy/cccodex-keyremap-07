
import sqlite3
import threading

import pytest
from fastapi.testclient import TestClient

import app as appmod
from app import (
    app, init_db, connect, run_conflict_replay, ReplayConflict,
    CONFLICT_PENDING, CONFLICT_CLOSED, REPLAY_SEMANTICS,
)


@pytest.fixture(autouse=True)
def _db(tmp_path):
    appmod._db_path_override = tmp_path / "t.db"
    init_db()
    yield
    appmod._db_path_override = None


@pytest.fixture()
def client():
    # 关闭服务端异常直抛，让 500 走真实 HTTP 响应路径以便断言回滚后的状态
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture()
def auth(client):
    r = client.post("/api/login", json={"username": "admin", "password": "123456"})
    return {"Authorization": "Bearer " + r.json()["token"]}


def _open_conflict(client, auth, old_key="OLD-ZZ"):
    """无映射入站 -> 冲突进队列，返回 conflict_id。"""
    r = client.post("/api/remap", headers=auth, json={"old_key": old_key})
    assert r.status_code == 409
    items = client.get("/api/conflicts", headers=auth).json()["items"]
    return next(x["id"] for x in items if x["old_key"] == old_key)


def _add_mapping(old_key, new_key, version="m-2026.11"):
    with connect() as c:
        c.execute("INSERT INTO mappings(version,old_key,new_key,kind) VALUES (?,?,?,?)",
                  (version, old_key, new_key, "补录"))


def _formal_rows(entity_key):
    with connect() as c:
        return [dict(r) for r in c.execute(
            "SELECT * FROM formal_entities WHERE entity_key=?", (entity_key,)).fetchall()]


def _audit(client, auth, conflict_id):
    return client.get(f"/api/conflict-replays?conflict_id={conflict_id}",
                      headers=auth).json()["items"]


# ---------- 夹具一：已正式键重放 -> 跳过架构无第二行且 skip 可查 ----------

def test_replay_skip_no_second_row_and_audit_queryable(client, auth):
    # 先让 NEW-200 进入正式区
    r = client.post("/api/remap", headers=auth, json={"old_key": "OLD-B1"})
    assert r.status_code == 200 and r.json()["action"] == "applied"
    assert len(_formal_rows("NEW-200")) == 1

    # 新入站键无映射进冲突区，随后补录映射指向已正式键
    cid = _open_conflict(client, auth)
    _add_mapping("OLD-ZZ", "NEW-200")

    # 重放：必须幂等跳过
    r = client.post(f"/api/conflicts/{cid}/replay", headers=auth)
    assert r.status_code == 200
    receipt = r.json()
    assert receipt["semantics"] == REPLAY_SEMANTICS == "idempotent_skip"
    assert receipt["outcome"] == "replay_skip"
    assert receipt["generation"] == 1
    assert receipt["conflict_status"] == CONFLICT_CLOSED

    # 正式区没有第二条
    rows = _formal_rows("NEW-200")
    assert len(rows) == 1
    assert rows[0]["mapped_from"] == "OLD-B1"  # 被跳过，没被覆盖

    # skip 审计可查，四要素齐备
    audit = _audit(client, auth, cid)
    assert len(audit) == 1
    a = audit[0]
    assert a["outcome"] == "replay_skip"
    assert a["conflict_id"] == cid and a["replay_at"] and a["generation"] == 1
    assert a["formal_id"] == rows[0]["id"]

    # 互证卡只读核证通过
    v = client.get(f"/api/conflicts/{cid}/verification", headers=auth).json()
    assert v["all_ok"] is True
    assert {x["name"] for x in v["checks"]} == {
        "登记闭合", "世代一致", "正式唯一", "无无痕覆盖", "争用留痕"}
    assert v["formal"]["entity_key"] == "NEW-200"


def test_replay_again_after_close_only_leaves_contention(client, auth):
    client.post("/api/remap", headers=auth, json={"old_key": "OLD-B1"})
    cid = _open_conflict(client, auth)
    _add_mapping("OLD-ZZ", "NEW-200")
    first = client.post(f"/api/conflicts/{cid}/replay", headers=auth)
    assert first.status_code == 200

    # 同 conflict_id 再放一次：争用，只留痕
    r = client.post(f"/api/conflicts/{cid}/replay", headers=auth)
    assert r.status_code == 409
    assert r.json()["outcome"] == "replay_contention"

    audit = _audit(client, auth, cid)
    outcomes = sorted(x["outcome"] for x in audit)
    assert outcomes == ["replay_contention", "replay_skip"]
    assert len(_formal_rows("NEW-200")) == 1  # 争用绝不写正式

    v = client.get(f"/api/conflicts/{cid}/verification", headers=auth).json()
    assert v["all_ok"] is True


# ---------- 首写路径：目标键不在正式区 ----------

def test_replay_applied_writes_generation_one(client, auth):
    cid = _open_conflict(client, auth, "OLD-QQ")
    _add_mapping("OLD-QQ", "NEW-999")
    r = client.post(f"/api/conflicts/{cid}/replay", headers=auth)
    assert r.status_code == 200
    receipt = r.json()
    assert receipt["outcome"] == "applied" and receipt["generation"] == 1

    rows = _formal_rows("NEW-999")
    assert len(rows) == 1 and rows[0]["generation"] == 1
    audit = _audit(client, auth, cid)
    assert [x["outcome"] for x in audit] == ["applied"]

    gens = client.get("/api/generations", headers=auth).json()["items"]
    g = next(x for x in gens if x["entity_key"] == "NEW-999")
    assert g["generation"] == 1 and g["replay_refs"] == 1


def test_replay_without_mapping_stays_pending(client, auth):
    # 种子冲突 OLD-C9 永远无映射
    cid = next(x["id"] for x in
               client.get("/api/conflicts", headers=auth).json()["items"]
               if x["old_key"] == "OLD-C9")
    r = client.post(f"/api/conflicts/{cid}/replay", headers=auth)
    assert r.status_code == 409
    assert r.json()["outcome"] == "no_mapping"

    conf = next(x for x in client.get("/api/conflicts", headers=auth).json()["items"]
                if x["id"] == cid)
    assert conf["status"] == CONFLICT_PENDING  # 业务性未成，不关闭
    v = client.get(f"/api/conflicts/{cid}/verification", headers=auth).json()
    assert v["all_ok"] is True  # 挂起态核证规则同样自洽


def test_replay_unknown_conflict_404(client, auth):
    assert client.post("/api/conflicts/9999/replay", headers=auth).status_code == 404
    assert client.get("/api/conflicts/9999/verification", headers=auth).status_code == 404


# ---------- 失败闭：审计与状态同事务回滚 ----------

def test_failure_rolls_back_formal_audit_and_status(client, auth, monkeypatch):
    cid = _open_conflict(client, auth, "OLD-QQ")
    _add_mapping("OLD-QQ", "NEW-999")

    def boom(*a, **kw):
        # 在正式 INSERT 之后、审计登记落库时注入失败
        raise RuntimeError("injected failure after formal write")

    monkeypatch.setattr(appmod, "_register_replay", boom)
    r = client.post(f"/api/conflicts/{cid}/replay", headers=auth)
    assert r.status_code == 500
    monkeypatch.undo()

    # 正式写入、审计登记、状态更新全部回滚
    assert _formal_rows("NEW-999") == []
    assert _audit(client, auth, cid) == []
    conf = next(x for x in client.get("/api/conflicts", headers=auth).json()["items"]
                if x["id"] == cid)
    assert conf["status"] == CONFLICT_PENDING

    # 回滚后可正常重放成功，证明没有留下半事务残骸
    r2 = client.post(f"/api/conflicts/{cid}/replay", headers=auth)
    assert r2.status_code == 200 and r2.json()["outcome"] == "applied"


# ---------- 并发：两路同放同一 conflict_id，只留一个终局 + 争用 ----------

def test_concurrent_replay_applied_single_winner():
    # 直接造一个挂起冲突 + 映射（不经 HTTP），目标键不存在
    with connect() as c:
        cur = c.execute("INSERT INTO conflicts(old_key,reason,status) VALUES (?,?,?)",
                        ("OLD-MT", "无映射", CONFLICT_PENDING))
        cid = cur.lastrowid
        c.execute("INSERT INTO mappings(version,old_key,new_key,kind) VALUES (?,?,?,?)",
                  ("m-2026.11", "OLD-MT", "NEW-MT1", "并发"))

    results, errors = [], []

    def worker():
        try:
            results.append(run_conflict_replay(cid)["outcome"])
        except ReplayConflict as e:
            errors.append(e.receipt["outcome"])

    threads = [threading.Thread(target=worker) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert results == ["applied"]          # 唯一赢家首写
    assert sorted(errors) == ["replay_contention"] * 5
    assert len(_formal_rows("NEW-MT1")) == 1  # 禁止双写正式

    with connect() as c:
        audit = [dict(r) for r in c.execute(
            "SELECT outcome FROM conflict_replays WHERE conflict_id=?", (cid,)).fetchall()]
        conf = c.execute("SELECT status FROM conflicts WHERE id=?", (cid,)).fetchone()
    assert sorted(x["outcome"] for x in audit) == (
        ["applied"] + ["replay_contention"] * 5)
    assert conf["status"] == CONFLICT_CLOSED


def test_concurrent_replay_skip_single_terminal():
    # 预置正式键，六线程并发重放：恰一条 replay_skip，其余争用
    with connect() as c:
        c.execute("INSERT INTO formal_entities(entity_key,mapped_from,version,generation,"
                  "created_at) VALUES (?,?,?,1,?)",
                  ("NEW-MT2", "OLD-SEED", "m-2026.11", "2026-11-11T00:00:00"))
        cur = c.execute("INSERT INTO conflicts(old_key,reason,status) VALUES (?,?,?)",
                        ("OLD-MT2", "无映射", CONFLICT_PENDING))
        cid = cur.lastrowid
        c.execute("INSERT INTO mappings(version,old_key,new_key,kind) VALUES (?,?,?,?)",
                  ("m-2026.11", "OLD-MT2", "NEW-MT2", "并发"))

    ok, contention = [], []

    def worker():
        try:
            ok.append(run_conflict_replay(cid)["outcome"])
        except ReplayConflict as e:
            contention.append(e.receipt["outcome"])

    threads = [threading.Thread(target=worker) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert ok == ["replay_skip"]
    assert sorted(contention) == ["replay_contention"] * 5
    assert len(_formal_rows("NEW-MT2")) == 1


# ---------- 入站侧幂等：UNIQUE 兜底，禁止重复插入第二条正式 ----------

def test_remap_is_idempotent_on_existing_formal_key(client, auth):
    r1 = client.post("/api/remap", headers=auth, json={"old_key": "OLD-B1"})
    r2 = client.post("/api/remap", headers=auth, json={"old_key": "OLD-B2"})  # 同指 NEW-200
    assert r1.json()["action"] == "applied"
    assert r2.json()["action"] == "idempotent_skip"
    assert len(_formal_rows("NEW-200")) == 1

    # 库层硬约束：手工插重复正式键必被拒
    with pytest.raises(sqlite3.IntegrityError):
        with connect() as c:
            c.execute("INSERT INTO formal_entities(entity_key,mapped_from,version,"
                      "generation,created_at) VALUES (?,?,?,1,?)",
                      ("NEW-200", "OLD-DUP", "m-2026.11", "2026-11-11T00:00:00"))


def test_health_declares_single_semantics(client):
    body = client.get("/api/health").json()
    assert body["replay_semantics"] == "idempotent_skip"
