# Agent 操作指南

JetRescue 是 Go + NATS JetStream 的任務派送與 DLQ／replay 練習專案：任務經 producer 或 MCP server 發布，worker 依 policy 啟動 Claude Code agent 執行後 ACK。設定、命令、資料流與排錯以 `README.md` 為準，本檔只列 agent 額外須遵守的規則。

現有介面是終端命令與 MCP server（`http://localhost:8080/mcp`，工具 `submitTask`）。沒有 PostgreSQL、REST API、agent inbox、result subject，也沒有 approval／decision 送交人類與核准後續跑的流程，勿當成現有功能。

## 執行環境

- 所有命令從 repository 根目錄執行；Go、Claude Code CLI 與 NATS CLI 都在 Compose 容器內，主機不需安裝。
- 非互動終端用 `docker compose exec -T`，避免 TTY 錯誤。
- `.env.dev` 不存在才從 `.env.example` 複製；已存在就保留，不覆寫、不輸出憑證。
- `CLAUDE_CODE_OAUTH_TOKEN` 只放 `.env.dev`；不得印出、貼到對話、寫進 Dockerfile 或提交。確認時只檢查是否有值，例如 `sh -c '[ -n "$CLAUDE_CODE_OAUTH_TOKEN" ] && echo set'`。
- 改環境檔後：`up -d --force-recreate` → bootstrap → 重啟 worker／listener／server（`restart` 不會載入新環境）。
- 改 `Dockerfile` 後需 `up -d --build`；重建 `app` 會中止容器內所有程序與使用者開的 shell，先確認範圍。
- 環境若原本由使用者啟動，結束任務時不要自行關閉。

## 常駐程序

worker、listener 與 MCP server 不會自行結束。優先用終端工具的背景 session 並保留 session ID；不支援時改用容器背景執行並寫日誌：

```bash
docker compose exec -d app sh -c 'exec go run ./cmd/worker > /tmp/jetrescue-worker.log 2>&1'
docker compose exec -d app sh -c 'exec go run ./cmd/advisory-listener > /tmp/jetrescue-listener.log 2>&1'
docker compose exec -d app sh -c 'exec go run ./cmd/server > /tmp/jetrescue-server.log 2>&1'
docker compose exec -T app tail -n 100 /tmp/jetrescue-worker.log
```

- 啟動前先用 `docker compose exec -T app ps -eo pid,args` 確認是否已在運行，避免重複啟動或覆蓋日誌。
- 背景模式的輸出只在日誌檔，`docker compose logs app` 看不到。
- `docker compose stop app` 會中止容器內所有工作，先確認範圍。
- worker 運行時，每筆有效任務都會啟動 agent 並消耗使用者的 Claude 訂閱額度；結束時告知使用者哪些程序仍在運行。

## 驗證與判讀

- 用可辨識的 `task_id`（如 `DLQ-AGENT-001`），比對操作前後的狀態與 payload；不要假設環境是空的。MCP 的 `task_id` 由 server 產生，需辨識時把標記寫在 `prompt` 開頭。
- 測試 agent 優先用 `readonly` 與簡短 prompt；只驗證重送／DLQ 時用空 `prompt` 等無效任務，worker 會在啟動 agent 前失敗，不消耗額度。
- producer 成功代表已收到 PubAck，不代表處理完成；需對照 worker 的 `agent finished`、ACK 紀錄與 consumer 狀態。
- agent 回覆全文只在容器內 `/root/.claude/projects/-tmp-jetrescue-<task_id>/<session_id>.jsonl`；worker log 只有 `result_chars`。回報結果時從 session 檔讀取，不要只依字數推斷。
- 回報權限行為時以 session 檔中的工具呼叫為準：`readonly` 實測仍會執行 `pwd`、`ls` 等唯讀 Bash，不得回報為只能用 Read／Grep／Glob。
- 失敗訊息須等投遞次數耗盡，確認 listener 的 `message quarantined` 與對應 DLQ envelope。
- Replay 前先讀 DLQ；`-sequence` 是 DLQ sequence，不是來源 sequence 或 `task_id`。同一操作重試沿用 request ID，新操作用新 ID。
- 無效任務（如空 `prompt`）replay 後仍會失敗，不得回報為任務恢復成功。
- 原始訊息與 advisory 必須仍保存在 stream。MISSION 使用 LimitsPolicy，不得擅自改成 WorkQueuePolicy。
- 不清空 streams、不刪 volume、不重設 consumer 來讓測試通過；破壞性操作須使用者授權。

## 修改程式

- 入口在各 `cmd/*/main.go`，只負責建立連線、設定並接線；共用邏輯放 `internal/`：`task`（payload、policy）、`messaging`（subject ↔ policy、發布）、`mcp`（MCP 工具）、`agent`（解析任務、啟動 claude CLI）。
- 新增 policy 須同時更新 `task/policy.go`、`messaging` 的 `SubjectFor` 與 `PolicyFromSubject`（兩者保持對稱）、`agent/policy.go` 的權限表與 README。
- agent 透過 `claude -p` 執行並以 `CLAUDE_CODE_OAUTH_TOKEN` 認證；不得加 `--bare`（bare mode 不讀此 token），不改用需要 API key 的 SDK。
- 使用者指定機制（如 NATS Go SDK）就照做，不以手動 CLI 建資源當前置步驟。
- 交付完整可編譯的程式與接入位置；只為重要架構或產品決策詢問，例行工具選擇直接決定。
- 連線與設定在入口建立後明確傳遞；網路操作與子程序帶 `context.Context`，共用邏輯回傳 error。
- 遵循 `gofmt`，套件名小寫，不建 `utils` 包。
- 可重現設定放 Dockerfile、Compose 與環境檔，不用臨時容器或命令前綴環境變數替代。
- 新增設定須同步更新 `.env.example`、本機環境檔與 README，保留使用者既有值。
- 改完執行 `docker compose exec -T app go build ./cmd/...`，再依範圍跑 README 的實際流程；編譯成功不等於端對端驗證。
- 回報實際執行的命令、觀察結果與未完成項目，明確指出環境阻擋或既有錯誤。
- 不提交憑證或本機環境檔；僅在使用者要求時 commit／push，訊息用 Conventional Commits。
