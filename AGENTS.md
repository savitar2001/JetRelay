# Agent 操作指南

JetRescue 是 Go + NATS JetStream 的 DLQ／replay 練習專案。設定、命令、資料流與排錯以 `README.md` 為準，本檔只列 agent 額外須遵守的規則。

目前只有終端操作；沒有 MCP server，也沒有 PostgreSQL、REST API、agent inbox，勿當成現有功能。

## 執行環境

- 所有命令從 repository 根目錄執行；Go 與 NATS CLI 都在 Compose 容器內，主機不需安裝。
- 非互動終端用 `docker compose exec -T`，避免 TTY 錯誤。
- `.env.dev` 不存在才從 `.env.example` 複製；已存在就保留，不覆寫、不輸出憑證。
- 改環境檔後：`up -d --force-recreate` → bootstrap → 重啟 worker／listener（`restart` 不會載入新環境）。
- 環境若原本由使用者啟動，結束任務時不要自行關閉。

## 常駐程序

worker 與 listener 不會自行結束。優先用終端工具的背景 session 並保留 session ID；不支援時改用容器背景執行並寫日誌：

```bash
docker compose exec -d app sh -c 'exec go run ./cmd/worker > /tmp/jetrescue-worker.log 2>&1'
docker compose exec -d app sh -c 'exec go run ./cmd/advisory-listener > /tmp/jetrescue-listener.log 2>&1'
docker compose exec -T app tail -n 100 /tmp/jetrescue-worker.log
```

- 啟動前先確認是否已在運行，避免重複啟動或覆蓋日誌。
- 背景模式的輸出只在日誌檔，`docker compose logs app` 看不到。
- `docker compose stop app` 會中止容器內所有工作，先確認範圍。

## 驗證與判讀

- 用可辨識的 mission ID（如 `DLQ-AGENT-001`），比對操作前後的狀態與 payload；不要假設環境是空的。
- producer exit 0 只代表 publish／flush 成功，沒有 PubAck，也不代表處理完成；需對照 worker 輸出與 consumer 狀態。
- 失敗訊息須等投遞次數耗盡，確認 listener 的 `message quarantined` 與對應 DLQ envelope。
- Replay 前先讀 DLQ；`-sequence` 是 DLQ sequence，不是來源 sequence 或 mission ID。同一操作重試沿用 request ID，新操作用新 ID。
- `force_fail=true` 的訊息 replay 後仍會失敗，不得回報為業務恢復成功。
- 原始訊息與 advisory 必須仍保存在 stream。MISSION 使用 LimitsPolicy，不得擅自改成 WorkQueuePolicy。
- 不清空 streams、不刪 volume、不重設 consumer 來讓測試通過；破壞性操作須使用者授權。

## 修改程式

- 主要邏輯仍在各 `cmd/*/main.go`，依需要再抽到 `internal/`。
- 使用者指定機制（如 NATS Go SDK）就照做，不以手動 CLI 建資源當前置步驟。
- 交付完整可編譯的程式與接入位置；只為重要架構或產品決策詢問，例行工具選擇直接決定。
- 連線與設定在入口建立後明確傳遞；網路操作帶 `context.Context`，共用邏輯回傳 error。
- 遵循 `gofmt`，套件名小寫，不建 `utils` 包。
- 可重現設定放 Compose 與環境檔，不用臨時容器或命令前綴環境變數替代。
- 新增設定須同步更新 `.env.example`、本機環境檔與 README，保留使用者既有值。
- 改完執行 `docker compose exec -T app go build ./cmd/...`，再依範圍跑 README 的實際流程；編譯成功不等於端對端驗證。
- 回報實際執行的命令、觀察結果與未完成項目，明確指出環境阻擋或既有錯誤。
- 不提交憑證或本機環境檔；僅在使用者要求時 commit／push，訊息用 Conventional Commits。
