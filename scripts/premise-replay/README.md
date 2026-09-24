# 前提回放（#875）

把一段固定的观察日志经真实的 `statements` 推送口（0054）送进 Utopia，由真实的 worker
处理，一站一站量「观察 → 开放陈述 → 类型化事实 → 时间线 → 规则结论」走到了哪里：计划
步骤的前提写成业务规则（0021），看依赖旧事实的步骤能不能在对的时刻退场、无关的步骤不动、
历史还在。

**这不是基准。** 它跑的是一段七个事件的日志，给的是行为与阻断点，不是性能数字。延迟只作
这一台机器、这一次运行的记录。

## 一条命令

```bash
REPLAY_DATABASE_URL=postgres://user:pass@host:5432/empty_db scripts/premise-replay/run.sh
```

需要 cargo、node ≥ 18、psql、curl。`REPLAY_DATABASE_URL` 指向一个**空的、只给这次用的**库：
脚本不建库也不删库，库里已经有表就拒绝跑；server 的 worker 会消费这个库里的任务，别的服务
或测试不能同时用它。可选：`REPLAY_PORT`（缺省 18751）、`REPLAY_OUT`（缺省
`target/premise-replay/<时间>`）、`REPLAY_SCHEDULER=0`（跳过定时推导那一段，那一段最长约
十五分钟）。

结果：`trace.jsonl`（每一次推送、介入、SSE 事件、每一站的读数，原始证据）、`summary.json`
（按库汇总）、`verification.json`（行为断言通过后才生成）、`server.log`。
回放会断言初始前提、重复推送、遮挡、移动、无关步骤、历史终点、重连、清理及 SSE 自动重读；
行为偏离预期或缺少任一必需阶段会以非零状态退出。已知缺口也作为当前行为明确断言，
不表示它们已经修好，也不表示无需人工的端到端闭环成立。

不连数据库的运行时回归：

```bash
node --test scripts/premise-replay/runtime.test.mjs
```

独立复核某次完整回放（包括清理后的自动 SSE 重读证据）：

```bash
node scripts/premise-replay/verify.mjs /absolute/path/to/summary.json
```

## 日志与步骤

`observations.json`：两样东西（cup-7、box-3）、两个位置（desk、shelf）。步骤 S_A「从桌上取
cup-7」写成一条归类规则：cup 的 `location` ∈ {desk} → `step_sa_ready`；S_B 读 box-3 自己的
`location`，是对照。`location` 声明为 functional 的状态属性。

事件依次是：初始观察、同一份载荷再推一遍、遮挡（一条「看不见」、没有位置）、移动、晚到的
旧观察（观察时间早于初始）、内容相同但 `doc_time` 变了、明确结束 B 的那条观察（墓碑 + 清理）。
同一段日志按两种身份各跑一遍：每次观察一个 `external_id`，与每样东西一个 `external_id`
（#875 回复里建议的做法）。

## 什么是真的，什么是补上的

没有配对话模型。推送、分块、按契约解析、写开放陈述、身份消解、物化、时间线、推导、SSE、
MCP 读取都是真实代码路径与真实 worker。对齐链上需要模型的几步这样补：

- **人经公开接口**：类别词绑到类（`POST /kbs/{id}/review/alignment/kind-words/{word}`）、
  短语签名绑到属性（`POST /kbs/{id}/review/alignment/phrases/{binding_id}`，每次有新观察后
  重定一次，以触发类型化物化）、时间线对账（`POST /kbs/{id}/ontology/relation-types/{id}/reconcile`）、
  立即推导（`POST /kbs/{id}/rules/run`）、关上一条事实（`POST /kbs/{id}/facts/{id}/close`）、
  清理墓碑（`POST /kbs/{id}/sources/{id}/missing/cleanup`）。
- **测试夹具（直接写库，一处）**：没有模型时对齐器从不写 `phrase_bindings`，对齐队列里没有
  短语签名可点，上面那条公开接口也就无从调用。夹具只写下「两票不一致、留给人」的那一行
  （`status = undecided`），不写任何结论。

每一处介入都记在 `trace.jsonl`（`intervention.*`）与 `summary.json` 的 `interventions` 里。
依赖夹具之后的结果与完整链路分开标注。

## 读取适配器

`replay.mjs` 里的 `Adapter` 只读不写：SSE 事件自动触发整体重读，突发事件合并，读取期间
若又收到事件则补读一次；首次连接和显式重连同样重读。回放驱动断开、重连和服务重启，
没有实现常驻进程的自动重连循环。清理后的验收等待 SSE 回调自行读到撤回，不调用手工重读。
决定只从重读到的权威状态里算（MCP `entity_facts`、REST 实体面板与证明、
Review 队列）。每个步骤给出 `continue`、`pause_reobserve` 或 `wait_confirmation` 之一和理由。
新鲜度是适配器自己的显式策略（`observations.json` 的 `freshness`），不是账本的 TTL。它不执行
任何动作；这里也没有一致快照或版本校验，「检查完到执行前」之间状态仍可能变。
