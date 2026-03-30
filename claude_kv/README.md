# claude_kv — Claude 专属工作目录

本目录由 Claude 维护，用于跨会话保持项目记忆、设计方向与自我纠错。

## 文件结构

| 文件 | 用途 |
|---|---|
| `design_*.md` | 每次设计方向文档（命名含版本/日期） |
| `progress.md` | 当前进展：已完成 / 未完成 / 阻塞项 |
| `lessons.md` | 错误记录与自我纠正（我或用户犯的错） |

## 使用规则

1. **上下文快耗尽时**：主动读取 `progress.md` + 最新 `design_*.md` + `lessons.md` 恢复记忆
2. **每次设计讨论后**：新建或更新对应 `design_*.md`
3. **每次阶段性完成后**：更新 `progress.md`
4. **发现错误/踩坑后**：立即追加到 `lessons.md`，不要等到会话结束

## 项目代号

**MassKV**（原名 FlowKV，研究代号 MassKV）
