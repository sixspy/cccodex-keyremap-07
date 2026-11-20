# 实体键桥

> 遵守 `ops/底座生成前必读_效率与技术栈.md`。FastAPI + Vue 3 + SQLite，交付须 docker compose 一键起；库用 SQLite，不挂外置 Postgres。  
> **禁止同构换皮**：本仓主链路、表结构、接口与页面均独立，不得与其它仓共用同一套 `/api/items` 壳。

## 骨架一句话

实体键映射生效后入站先重映再进正式区，冲突进队列，映射版本与正式区对账

## 基本信息

| 项 | 值 |
|----|-----|
| 产品名（首页） | 实体键桥 |
| 目录 | `07-keyremap` |
| 领域建议 | 数仓与数据湖 |
| 接口 | http://127.0.0.1:10917 |
| 库 | `data/app.db` |
| 账号 | admin / 123456 |

## 为何能出多题

一对多/多对一、生效窗、旧键只读、重映重放

## 避开

别做成 Feed 冲突归并；正式区不是 staging 换皮

## 互斥切口候选

- 冲突拒收整批 vs 进冲突区后重放
- 废弃旧键只读保留 vs 重写正式键
- 生效窗原子切换 vs 双窗按事件时裁

## 启动（一键）

`powershell
cd projects/07-keyremap
docker compose up -d --build
`

浏览器：http://127.0.0.1:10917/  
健康检查：GET http://127.0.0.1:10917/api/health

## 冲突重放（唯一正确性口径：甲路径 · 幂等跳过）

全局只贯彻一种口径，`replay_semantics=idempotent_skip`，乙路径（修订覆盖/抬 generation/被盖快照）明确不采用，两边不混用：

- 目标新键已在正式区 → **跳过写入**，只追加 `replay_skip` 重放登记，冲突关闭；正式区 `entity_key` 有 UNIQUE 索引兜底，禁止第二条正式行
- 目标新键不存在 → 首写正式区（generation=1），登记 `applied`，冲突关闭
- 已关闭冲突再放 → 不动正式、不动状态，只追加 `replay_contention` 争用登记（HTTP 409，回执仍在顶层 JSON）
- 重放时仍无映射 → 登记 `no_mapping`，冲突保持挂起
- 单事务 `BEGIN IMMEDIATE`：正式写入、重放登记、冲突状态同生共死，任一失败整体回滚；禁止无审计无痕覆盖

落库形态采用**重放登记表** `conflict_replays`（conflict_id + replay_at + outcome + generation 四要素齐备，只追加）。

| 接口 | 说明 |
|------|------|
| `POST /api/conflicts/{id}/replay` | 执行重放，回执含 outcome/generation/conflict_status |
| `GET /api/conflict-replays?conflict_id=` | 重放审计流水（只读，可按冲突过滤） |
| `GET /api/conflicts/{id}/verification` | 重放结局核证互证卡（只读）：冲突区/正式区/审计/世代五方对质，五项核证 |
| `GET /api/generations` | 互证世代列表（只读），含每条正式行被登记引用次数 |

并发安全：同一 conflict_id 的并发重放由 SQLite 写锁串行化，恰一个终局（applied 或 replay_skip），后到者全部只留 contention。

本机开发（可选）：

`powershell
python -m pip install -r requirements.txt
python -m uvicorn app:app --app-dir backend --host 127.0.0.1 --port 10604
python -m pytest -q
`
