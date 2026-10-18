# JetRescue

用 NATS JetStream 練習失敗訊息的重送、隔離與重新發布。送入一筆正常或故意失敗的 mission，觀察 worker 的 ACK／重送行為，再從 DLQ 取回原文並 replay。

目前介面是 Go 指令。人類與具備終端操作能力的 agent 都使用下面的命令；MCP server 尚未實作。

## 1. 準備環境與設定

需要 Docker Desktop（或 Docker Engine）與 Docker Compose。Go 與 NATS CLI 已包含在 Compose 服務中，主機不需要另外安裝。

下載或 clone 本 repository 後，進入 repository 根目錄（包含 `go.mod` 與 `docker-compose.yml` 的目錄）。以下所有命令均在此目錄執行。

首次使用，複製設定範例；已有 `.env.dev` 時保留原檔，依下表補齊設定：

```bash
cp .env.example .env.dev
```

| 變數 | 範例／預設值 | 用途 |
| --- | --- | --- |
| `APP_ENV` | `dev` | Compose 預設選用 `.env.dev`，其他環境的選法見下方 |
| `NATS_URL` | `nats://nats:4222` | 必填；容器透過服務名稱 `nats` 連線 |
| `MISSION_ACK_WAIT` | `30s` | 每次投遞後等待 ACK 的期限；須為正的 Go duration，例如 `5s`、`1m` |
| `MISSION_MAX_DELIVER` | `3` | 同一訊息最多投遞幾次，包含第一次；須為正整數 |

想快一點看到重送，可在 `.env.dev` 把 `MISSION_ACK_WAIT` 改成 `5s`。consumer 政策由 bootstrap 套用；producer 雖然共用設定載入函式，發送時只使用連線設定。

預設命令使用 `.env.dev` 作為容器環境。要使用其他環境，例如 `.env.testing`，請在檔案內設定 `APP_ENV=testing`，並在**每一條** Compose 命令加上相同的 `--env-file`：

```bash
docker compose --env-file .env.testing up -d
```

只在自訂環境檔內填入 `APP_ENV`，不會讓 Compose 自動找到該檔案。環境檔可能包含憑證，請保留在本機；`.env.dev` 已被忽略，自訂環境檔也須加入 `.gitignore`。

## 2. 啟動環境與初始化

```bash
docker compose up -d
docker compose ps
docker compose exec app go run ./cmd/bootstrap
```

首次執行會下載 images 與 Go dependencies。bootstrap 成功時會輸出 `JetStream bootstrap completed`。

| Compose 服務 | 功能 |
| --- | --- |
| `app` | Go 開發容器，掛載本機程式碼，保持待命供執行 `go run` 等命令 |
| `nats` | 啟用 JetStream 的 NATS Server，資料保存在 `nats-lab-data` volume |
| `nats-tools` | 常駐的 NATS CLI 容器，供發布訊息及檢查 streams／consumers |

Compose 會建立所需的 volume，不用手動建立。`up -d` 只啟動環境，**不會自動執行 worker 或 listener**。

開兩個終端，分別執行以下命令並保持運行。第三個終端用來發布與查詢訊息。

終端 A，啟動 worker：

```bash
docker compose exec app go run ./cmd/worker
```

預期看到 `MISSION worker started`。

終端 B，啟動 listener：

```bash
docker compose exec app go run ./cmd/advisory-listener
```

預期看到 `advisory listener started`。listener 會在啟動時建立自己的 `MISSION_DLQ_COLLECTOR` consumer。

## 3. 發送一筆正常訊息

在第三個終端執行：

```bash
docker compose exec app go run ./cmd/producer
```

producer 固定發布到 `mission.created`，內容如下，目前沒有自訂 payload 的命令列參數：

```json
{"mission_id":"A-1001","force_fail":false}
```

worker 會記錄 `processing mission` 並 ACK。可以查詢 consumer 的 pending／ack pending 狀態：

```bash
docker compose exec nats-tools nats consumer info MISSION MISSION_WORKER
```

producer 成功只表示 publish／flush 成功；目前沒有等待 JetStream PubAck，也不代表 worker 已完成。請搭配 worker 輸出與 consumer 狀態判讀。

## 4. 自訂 payload，觀察重送與 DLQ

透過 NATS CLI 發送一筆故意失敗的訊息：

```bash
docker compose exec nats-tools nats pub mission.created '{"mission_id":"DLQ-DEMO-001","force_fail":true}'
```

`mission_id` 必須非空。`force_fail=true` 會讓 worker 回報錯誤並保持未 ACK。預設最多投遞 3 次，各次之間等待 `AckWait`，最後由 listener 隔離到 DLQ。請等到 listener 出現 `message quarantined`，再檢查：

```bash
docker compose exec nats-tools nats stream info MISSION_DLQ
docker compose exec nats-tools nats stream get MISSION_DLQ 1
```

`1` 是 **DLQ stream 的 sequence**。已有資料時，先從 stream info 確認 First／Last Sequence，再換成要查的實際編號；DLQ sequence 與來源 `MISSION` sequence 不一定相同。

DLQ envelope 包含 `quarantine_id`、`source.stream`、`source.consumer`、`source.subject`、`source.stream_seq`、時間、headers 與 `payload_base64`。比對來源與原始 payload，確認是本次送入的訊息。

NATS CLI 會使用容器中的 `NATS_URL`，與 Go 指令連到相同 server。

## 5. Replay 單筆 DLQ 訊息

```bash
docker compose exec app go run ./cmd/replayer -sequence 1 -request-id replay-demo-001
```

| 參數 | 意義 |
| --- | --- |
| `-sequence` | 必填、正整數；要讀取的 `MISSION_DLQ` sequence |
| `-request-id` | 必填；同一次操作重試時維持相同值，新一次操作使用新值 |

成功時會輸出 `DLQ message replayed`，包含原 subject、新的 stream sequence 與 replay ID。

replay 還原原始 payload 與 headers，發布回原 subject，**不刪除 DLQ 紀錄，也不修改 payload**。原文若仍是 `force_fail=true`，worker 會再次失敗並可能產生新的 DLQ 紀錄；這表示 replay 路徑運作，不表示業務錯誤已修復。

同一 request ID 的去重依賴 JetStream duplicate window，不能視為永久的 exactly-once 保證。

## 6. 驗證 listener 停機後補處理

1. 在終端 B 用 `Ctrl+C` 停止 listener，保持 worker 與 NATS 運行。
2. 使用前面的發布命令，換一個新的 `mission_id` 送入失敗訊息。
3. 等待重試耗盡，確認 event stream 有新增事件；DLQ 此時不應出現該筆訊息。
4. 重新執行 listener 命令，確認出現 `message quarantined`，並在 DLQ 找到對應來源。

檢查命令：

```bash
docker compose exec nats-tools nats stream info MISSION_MAX_DELIVERY_EVENTS
docker compose exec nats-tools nats stream info MISSION_DLQ
docker compose exec nats-tools nats consumer info MISSION_MAX_DELIVERY_EVENTS MISSION_DLQ_COLLECTOR
```

比較操作前後的 sequence／訊息數與 payload，避免把歷史資料當成本次結果。原始訊息與 advisory 必須仍保存在各自 stream，listener 才能補處理。

## 7. 修改設定、停止與排錯

修改 `.env.dev` 後，先停止 worker／listener，再重新建立使用環境檔的容器、執行 bootstrap，最後重啟 worker／listener：

```bash
docker compose up -d --force-recreate app nats-tools
docker compose exec app go run ./cmd/bootstrap
```

單純 `docker compose restart` 不會載入新的容器環境設定。bootstrap 會更新 mission consumer 政策；已存在的 streams 只會確認存在，不會自動修改設定。

| 現象 | 檢查方式 |
| --- | --- |
| 找不到 `.env.dev` | 從 `.env.example` 建立設定檔，確認位於 Compose 檔案旁 |
| 連不上 NATS | 檢查 `docker compose ps` 與 `docker compose logs nats`；容器內使用 `nats://nats:4222` |
| 找不到 stream／consumer | 先執行 bootstrap；collector consumer 在 listener 啟動時建立 |
| 訊息沒被處理 | 確認 worker 正在運行，subject 為 `mission.created`，且 `mission_id` 非空 |
| 沒進 DLQ | 確認 `force_fail=true`、已等到投遞次數耗盡、listener 正在運行 |
| 從主機連 NATS | 使用 `nats://localhost:4222`；`http://localhost:8222` 是監控入口，不是業務 API |

用 `Ctrl+C` 停止各個前景 Go 指令，再關閉環境：

```bash
docker compose down
```

這會保留命名 volume 中的 JetStream 資料，下次啟動仍可讀取歷史訊息。

## 專案結構與資料流

```text
.
├── README.md                     # 使用、設定與排錯指南
├── AGENTS.md                     # agent 操作與修改本專案的規則
├── docker-compose.yml            # app、NATS、NATS CLI 與持久化 volume
├── .env.example                  # 可提交的設定範例
├── .env.dev                      # 本機設定，不提交
├── bootstrap.sh                  # 容器內 bootstrap 的簡易入口
├── go.mod / go.sum               # Go 版本與依賴
├── cmd/
│   ├── bootstrap/main.go         # 建立 streams 與 mission consumer
│   ├── producer/main.go          # 發布固定的正常 mission
│   ├── worker/main.go            # 處理 mission，成功後 ACK
│   ├── advisory-listener/main.go # 找回重試耗盡的訊息並發布至 DLQ
│   └── replayer/main.go          # 依 DLQ sequence 發布回原 subject
└── internal/
    └── config/nats.go            # 載入與驗證連線／consumer 設定
```

```mermaid
flowchart LR
    P[producer / NATS CLI] -->|mission.created| M[MISSION]
    M --> W[MISSION_WORKER / worker]
    W -->|成功 ACK| Done[完成]
    W -->|未 ACK，逾時重送| W
    W -->|耗盡 MaxDeliver，server 發 advisory| E[MISSION_MAX_DELIVERY_EVENTS]
    E --> L[advisory-listener]
    M -->|依來源 sequence 取回原文| L
    L --> D[MISSION_DLQ]
    D --> R[replayer]
    R -->|原 subject / payload| M
```

| 資源 | 設定 |
| --- | --- |
| `MISSION` | 收錄 `mission.>`，使用 Limits retention |
| `MISSION_WORKER` | durable pull consumer，過濾 `mission.created`，explicit ACK |
| `MISSION_MAX_DELIVERY_EVENTS` | 收錄 `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.MISSION.MISSION_WORKER` |
| `MISSION_DLQ_COLLECTOR` | listener 的 durable consumer，處理成功才確認 advisory |
| `MISSION_DLQ` | 收錄 `dlq.mission.>`，保存隔離 envelope |

## 開發與目前範圍

```bash
docker compose exec app go build ./cmd/...
```

此命令確認所有 Go 入口可以編譯。實際行為請依前面的正常處理、DLQ、replay 與 listener 補處理流程驗證。

本版介面是 Go 指令與固定的 mission schema，尚未提供 MCP、agent 身分／專屬 inbox、PostgreSQL、REST API 或批次 replay。這是本機開發環境，NATS 連接埠目前未設定驗證或 TLS。後續會以此 DLQ 底座發展 agent 可用的 MCP 工具。
