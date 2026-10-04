# 区域地震台网告警可靠投递中继

把已确认地震告警**可靠送入应急广播网关**：网络闪断、超时、5xx 或发送进程重启
都不会造成告警丢失、被篡改或被重复接纳。

* 纯 Python 3.11 标准库实现，镜像内**零第三方依赖**。
* 存储为 SQLite（WAL），收发两端各自持久化；崩溃重启后自动恢复并幂等收敛。
* `docker compose up` 启动 **API** 与 **接收模拟器**，均带健康检查；
  一次性服务 **verify** 汇总“代码测试 + 构建检查 + 投递冒烟”，退出码即结论。

## 快速开始

```bash
# 可选：配置宿主机端口与共享密钥（见 .env.example）
cp .env.example .env

docker compose up -d --build

# 一键验证（退出码 0 表示构建检查、21 项代码测试、8 个投递冒烟全部通过）
docker compose run --rm verify
echo $?

# 只跑 API + 接收模拟器时，API 宿主机端口由 API_HOST_PORT 控制（默认 8080）
API_HOST_PORT=18080 docker compose up -d
```

## 接口

### 提交告警 `POST /api/alerts`

```json
{
  "alertKey": "EQ-20261004-0001",
  "station": "台网-首都圈-BJ01",
  "sequence": 1024,
  "level": "red",
  "observedAt": "2026-10-04T13:30:00+08:00",
  "reading": 6.8
}
```

* 首次受理 → `201`：`{ "alertId": "alt-…", "deliveryId": "dlv-…", "status": "pending" }`
* **同 alertKey 同内容**（字段顺序无关）→ `200`，`replayed: true`，原样回放同一
  `alertId`/`deliveryId`，绝不二次接纳；
* **同 alertKey 异内容** → `409 Conflict`（防同键告警被悄悄改读）。

### 查询 `GET /api/alerts/{alertId}`

返回 `status`（`pending | delivering | delivered | failed`）、`attempts`
尝试次数、`lastResult`/`lastHttpStatus` 最近结果，并给值班员一句明确结论：

* 成功：`✅ 告警已被应急广播网关唯一接纳（deliveryId 幂等确认）`
* 失败：`❌ 最终失败：重试耗尽…` 或 `❌ 最终失败：接收端返回不可重试响应（HTTP 4xx）…`

## 可靠性设计（如何满足 exactly-once 收敛）

| 风险 | 机制 |
| --- | --- |
| 重试导致重复接纳 | 每次投递带同一 `X-Delivery-Id`，接收端以其为唯一幂等键落库；重放返回 `duplicate:true`，同 deliveryId 异内容返回 `409`（篡改检测）。 |
| 请求体被改动 | 请求体规范化（字段排序的 JSON）后算 SHA-256 指纹；首次受理时用共享密钥生成 HMAC-SHA256 签名并**入库一次**，之后每次重试原样重放完全相同的 body/deliveryId/签名。 |
| 网络闪断/超时/5xx | 视为结果未知并重试：首次尝试 + 最多 3 次重试（共 4 次），指数退避。 |
| 对方已接纳后才断连 | 客户端只看到断连，重试命中接收端幂等记录，`delivered` 收敛且只接纳一次。 |
| 4xx（如签名不符 401、未找到 404） | 不可重试，**立即 failed**，`attempts=1`。 |
| 发送进程重启/崩溃 | 非终态（pending/delivering）记录在启动时全部恢复；终态不可被改写。接收端去重表也持久化，跨重启仍只接纳一次。 |
| 并发重复提交 | 受理事务 `BEGIN IMMEDIATE` + `alert_key UNIQUE`；投递池单飞去重，周期兜底扫描卡死记录。 |

## 接收模拟器

`relay.receiver` 提供 `/gateway/alerts`（带健康检查）与管理接口（仅演练/测试用）：

```bash
# 前 2 次返回 503 后恢复
curl -sX POST localhost:8081/admin/faults -H 'Content-Type: application/json' \
  -d '{"mode":"http_error","count":2,"status":503}'
# 先落库接纳、再断开连接（结果未知场景）
curl -sX POST localhost:8081/admin/faults -H 'Content-Type: application/json' \
  -d '{"mode":"drop_after_accept","count":1}'
# 支持：ok / always(默认404) / http_error / drop_before_accept / drop_after_accept / stall
```

## 目录结构

```
relay/
  signing.py     规范化、指纹、HMAC-SHA256 签名与校验
  store.py       AlertStore（告警/尝试审计）、ReceiverStore（deliveryId 去重）
  httpclient.py  投递结果分类（accepted/unretryable/retryable）与退避策略
  api.py         POST/GET API + 投递 worker 池 + 重启恢复
  receiver.py    应急广播网关接收模拟器（验签、幂等、故障注入）
  scenarios.py   8 个端到端冒烟场景
  client.py      冒烟/测试共用 HTTP 工具
tests/           21 个单元与端到端测试（含真实进程重启恢复）
scripts/verify.py 一次性 verify：构建检查 + 单测 + 冒烟，位掩码退出码
Dockerfile / docker-compose.yml / .env.example
```

## 本地开发（无 Docker）

```bash
python3 -m unittest discover -s tests -v          # 21 项测试
python3 -m relay.receiver --port 8081 --db /tmp/r.db --secret dev
RECEIVER_HOST=127.0.0.1 RECEIVER_PORT=8081 SHARED_SECRET=dev \
  python3 -m relay.api --port 8080 --db /tmp/a.db
API_URL=http://127.0.0.1:8080 RECEIVER_ADMIN=http://127.0.0.1:8081 \
  python3 scripts/verify.py
```
