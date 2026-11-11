# AA 窗口 IPC

```text
AA NEXT     → TS 守护进程 ── ACP ──→ 扩展版 codex-acp ── IPC ──→ Codex App／IDE
```

| 模块 | 职责 |
| --- | --- |
| `WindowTransport` | Unix socket、帧、握手、请求配对和对端发现 |
| `WindowFollower`、`WindowSnapshot` | 跟随指定 owner，按 revision 更新状态，将窗口快照转换为原生历史项 |
| `WindowSession` | thread 身份、当前控制路径、活跃 turn、稳定消息 ID 和送达对账 |
| `WindowAcpSession` | 将窗口事件、审批和设置投影到已有 ACP 会话 |
| `WindowOwner` | 公布本适配器持有的会话，处理窗口请求和排队文本 |
| `SharedAppServer` | 以 WebSocket 连接已有共享 daemon，复用 App Server 客户端 |

窗口路由和 turn 活动独立变化。断线或 patch 缺口会停用控制路径，重新发现 owner 或获取快照。等待送达的输入通过原生消息 ID 对账，超时后不会自动重发。只有确认无窗口 owner、窗口 socket 不存在或连接被拒绝时，新建的跟随尝试才交给 App Server 处理，原生 writer lock 裁决写入资格。AA 的宿主负责预览进程影响、接受确认和恢复原 thread。

ACP 的历史读取、路由探测和消息 ID 扩展需要先协商 `aa/sessionRead`。ACP v1 通过 `session_info_update._meta.aa/windowActivity` 接收外部 turn 状态，v2 使用原生会话状态。窗口模式目前支持文本输入、命令和文件审批、模型与推理强度。App Server 路径继续提供上游 ACP 支持的操作。

## 方法版本

参考版本为 macOS Codex App `26.1103.60933`（CLI `0.167.0-alpha.3.2`）、VS Code `1.140.0` 搭配 Codex 扩展 `26.1106.51428`（CLI `0.167.0-alpha.4.1`），以及共享 daemon 和 TUI 验证使用的 CLI `0.163.0`。App 的方法表来自 `.vite/build/early-bootstrap.js`，SHA-256 为 `5fb7f97bb289c4672d3d9e0cbb5e3352ecafa9c9224de20825b2a9f9770fa8ec`。IDE 的方法表来自 `out/extension.js`，SHA-256 为 `30fb21db73fff04c505dc1463dc562205c2321fea52a615d8c2658fd356d157e`。

| 方法 | App | IDE |
| --- | --- | --- |
| `initialize` | 0 | 0 |
| `thread-owner-discovery` | 1 | 1 |
| `thread-stream-following-changed`、`thread-stream-following-status-requested`、`ipc-connection-reset` | 1 | 1 |
| `thread-stream-state-changed` | 11 | 11 |
| `thread-follower-start-turn` | 2 | 2 |
| `thread-follower-steer-turn` | 1 | 1 |
| `thread-follower-interrupt-turn` | 4 | 4 |
| `thread-follower-load-complete-history`、`thread-follower-read-model-settings` | 1 | 1 |
| `thread-follower-update-thread-settings` | 2 | 2 |
| `thread-follower-command-approval-decision`、`thread-follower-file-approval-decision` | 1 | 1 |
| `thread-follower-set-queued-follow-ups-state` | 1 | 3 |
| `thread-follower-remove-queued-message`、`thread-follower-clear-queued-messages` | 1 | 1 |
| `thread-queued-followups-changed` | 2 | 3 |

## 回归

上游基线为 `e981b3c`。升级上游时，先核对 ACP 初始化、会话加载、取消、审批和连接关闭的入口，再对照 App 和 IDE 的版本表及对应请求体。使用独立 Codex home 和临时 thread 验证。版本一致时也应检查 payload 变化，例如 IDE 的插话 `restoreMessage`、`steeringUserMessage` 落地和 `preparationMessageId`。

`npm run build && python3 scripts/verify-window-ipc.py` 使用临时 Unix socket 验证 ACP v1 和 v2。fixture 故意丢弃发送和插话的 RPC 回执，以 owner 发布的状态确认送达。它也验证文件审批、原 turn 中断和 follower 关闭。将真实 App 或 IDE 接入时，通过所属 Codex home 的 `ipc/ipc.sock` 验证 owner 发现、原 thread 写入和重新订阅。

| 2026-11-11 实测入口 | 验证结果 |
| --- | --- |
| macOS Codex App | 通过实际 App 的 IPC 路由发现独立测试适配器 owner，并通过它继续原 App Server thread |
| VS Code 官方 Codex 扩展 | Effi `AgentProcess` 通过 ACP v1 发现 IDE owner，读取历史、续发、接收实时输出、切换模型、插话、中断和重新跟随。真实命令审批的拒绝结果返回了原 turn。ACP v2 验证适配器 stdio 断开时原 turn 继续运行，重新连接后可中断。独立 App Server 能读取同一 thread，尝试 resume 时被 `thread_active_writer` 拒绝。IDE 也以 follower 打开适配器持有的 thread。通过真实 IDE 路由重复提交同一条 v3 排队消息，只落地一次 |
| macOS Codex TUI | TUI 与 ACP 连接同一个 App Server daemon，打开相同的 thread，ACP 续发后从原 thread 读回历史 |
| Linux agentd | 两个真实内核写锁由同一个一次性测试进程持有，预览列出两个 thread。拒绝跨设备及重放确认，使用 pidfd 通知原进程并验证锁释放 |

窗口和共享 daemon 入口使用当前用户的私有 Unix socket。Linux 上可使用 agentd 的 pidfd 恢复流程。Windows 的窗口命名管道和非 Linux 的进程终止恢复尚未接入。
