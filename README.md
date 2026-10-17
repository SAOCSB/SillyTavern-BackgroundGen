# SillyTavern 后台生成（Background Generation）

关掉网页、锁屏、切到后台或者断网之后，回复依然会在服务器上**完整生成**；回来时自动补上，几乎感觉不到中断。

> **English:** A SillyTavern extension that keeps generations running on the server when you close the tab, lock your phone or lose connection. When the page is alive it transparently resumes the stream from where it broke; if the page was closed, the finished reply is written into the chat the next time you open it — through SillyTavern's own post-processing pipeline (regex, reasoning parsing, `MESSAGE_RECEIVED` events, save). Requires the companion server plugin [SillyTavern-BackgroundGen-Server](https://github.com/SAOCSB/SillyTavern-BackgroundGen-Server).

本仓库是**前端扩展**，必须配合服务端插件 [SillyTavern-BackgroundGen-Server](https://github.com/SAOCSB/SillyTavern-BackgroundGen-Server) 使用。

## 效果

| 场景 | 表现 |
|---|---|
| 页面一直开着 | 和原版完全一样 |
| 锁屏 / 切后台 / 断网后回来 | 从断点续传，SillyTavern 感觉不到中断，不会重复请求模型 |
| 关掉或刷新网页后再打开 | 打开那个聊天时，完整回复自动补上 |
| 回来时还在生成 | 输入框上方显示进度和「停止」按钮，生成完自动显示 |
| 换一台设备打开同一个聊天 | 原设备还连着就等它完成再同步；原设备掉线了就由新设备接手 |
| 点停止 | 和原版一样保留已生成的部分，服务器同时停止生成 |
| 服务端插件不可用 | 自动退回原版行为 |

## 安装

在 SillyTavern **1.19.0** 上开发和测试，更早的版本没有测试过。

### 1. 安装服务端插件

```bash
cd SillyTavern/plugins
git clone https://github.com/SAOCSB/SillyTavern-BackgroundGen-Server
```

然后在 `config.yaml` 里打开服务端插件：

```yaml
enableServerPlugins: true
```

**重启 SillyTavern**。启动日志里出现 `[bg-gen] v1.0.0 ready` 就说明插件加载成功。

### 2. 安装本扩展

在 SillyTavern 里打开 **扩展 → 安装扩展**，填入：

```
https://github.com/SAOCSB/SillyTavern-BackgroundGen
```

### 3. 确认

刷新网页，在扩展面板里找到 **后台生成**，看到「服务端插件已连接」就可以用了。不需要任何额外操作，正常聊天即可。

## 设置

- **启用后台生成**：关掉后完全按原版方式生成。
- **补写回复时显示提示**：关闭页面期间生成的回复被写入时弹出提示。
- **待处理的后台回复**：列出还没写入聊天的后台结果，可以手动丢弃。

## 工作原理

1. 扩展拦截 SillyTavern 发往自身生成接口的请求，交给服务端插件。插件以同一用户、同一会话的身份调用 SillyTavern 自己的生成接口，请求内容**原样转发**，并把返回的数据完整缓存下来。浏览器断开不会中止这次生成。
2. 页面还在时，连接断了就带着已接收的字节数重新连接，从断点继续。SillyTavern 自己的流式处理照常进行。
3. 页面已经关闭时，结果保存在服务器上（最多 7 天）。下次打开对应聊天，扩展用 SillyTavern 自带的收尾流程写入回复：`cleanUpMessage` → `saveReply` → `MESSAGE_RECEIVED` / `CHARACTER_MESSAGE_RENDERED` 事件 → 保存。正则、思维链解析、依赖这些事件的扩展（变量更新、记忆、表格等）都会照常处理这条回复。
4. 写入前会核对聊天当前的状态。如果页面关闭前已经存下了半截回复，会被替换成完整版本；如果聊天里已经有了新内容，会弹窗让你选择追加到末尾、丢弃或以后再说。

## 兼容性

- 中转覆盖所有经过 SillyTavern 生成接口的方式：Chat Completion（任意来源）、Text Completion、KoboldAI、NovelAI。**不支持 AI Horde**。
- Chat Completion 经过完整测试；Text Completion / KoboldAI / NovelAI 的断线续传走同一套机制，但「关页面后补写」这部分还没有充分测试，欢迎反馈。
- 支持普通生成、重新生成、滑动（swipe）、继续、流式与非流式、群聊、思维链。
- 不修改提示词，与其他扩展、预设、角色卡互不干扰。开发时与酒馆助手（JS-Slash-Runner）、小白X（LittleWhiteBox）、记忆增强（st-memory-enhancement）等扩展同时加载测试过。

## 已知限制

- SillyTavern 重启时，正在生成的回复会中断（已经生成完、尚未写入的结果不受影响）。
- 页面关闭期间，群聊的自动轮流发言只会完成当前这个角色；自动继续、自动滑动不会触发。
- 生成的消息里会带一个很小的 `extra.bg_gen` 标记，用来识别半截回复，不会发给模型。

## 卸载

在扩展管理里删除本扩展；删除 `plugins/SillyTavern-BackgroundGen-Server` 目录，或把 `enableServerPlugins` 改回 `false`，然后重启 SillyTavern。

## 许可证

[MIT](LICENSE)
