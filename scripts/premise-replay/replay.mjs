#!/usr/bin/env node
// 前提回放（#875）：一段固定的观察日志经真实的 statements 推送口进 Utopia、由真实的 worker
// 处理，一站一站量「观察 → 开放陈述 → 类型化事实 → 时间线 → 规则结论」走到了哪里。
//
// 每一站之后读权威状态（REST 实体面板、MCP entity_facts、规则命中、证明、Review 队列、
// 必要处直接查库做诊断），由一个最小的读取适配器给出每个步骤的确定性决定：
// continue / pause_reobserve / wait_confirmation。它不执行任何动作，也不发任何带副作用的外部请求。
//
// 没有配对话模型。对齐链上需要模型的几步由人经公开接口补上；公开接口够不着的那一步（短语
// 签名在队列里得先有一行）由一个标明了的测试夹具补上。每一处介入都记在 trace 里，结果与
// 完整链路分开标注。
//
// 用法见同目录 README（run.sh 起 server、连一个一次性的空库，再调这里）。

import fs from "node:fs";
import path from "node:path";
import { execFileSync } from "node:child_process";

const HERE = path.dirname(new URL(import.meta.url).pathname);
const BASE = process.env.REPLAY_BASE || "http://127.0.0.1:18751";
const OUT = process.env.REPLAY_OUT || path.join(HERE, "out");
const RESTART_DIR = process.env.REPLAY_RESTART_DIR || "";
const SCHEDULER = process.env.REPLAY_SCHEDULER !== "0";
const LOG = JSON.parse(fs.readFileSync(path.join(HERE, "observations.json"), "utf8"));
const API = "/api/v1";

fs.mkdirSync(OUT, { recursive: true });
const T0 = Date.now();
const traceFile = fs.createWriteStream(path.join(OUT, "trace.jsonl"));

/** 一条记录：相对起点的毫秒、墙钟、种类、数据。trace 是原始证据，报告只引用它 */
function rec(kind, data = {}) {
  const row = { ms: Date.now() - T0, wall: new Date().toISOString(), kind, ...data };
  traceFile.write(JSON.stringify(row) + "\n");
  return row;
}
function say(...parts) {
  console.log(`[${((Date.now() - T0) / 1000).toFixed(1).padStart(6)}s]`, ...parts);
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const iso = (s) => new Date(s).toISOString();

// ---------------------------------------------------------------------------
// 连接：HTTP（会话 JWT）、MCP（个人令牌）、诊断用的 psql
// ---------------------------------------------------------------------------

let JWT = "";

async function http(method, url, { body, bearer, ok = [] } = {}) {
  const headers = {};
  const auth = bearer ?? JWT;
  if (auth) headers.authorization = `Bearer ${auth}`;
  let payload;
  if (body !== undefined) {
    headers["content-type"] = "application/json";
    payload = typeof body === "string" ? body : JSON.stringify(body);
  }
  const started = Date.now();
  const r = await fetch(BASE + url, { method, headers, body: payload });
  const text = await r.text();
  let json = null;
  try {
    json = text ? JSON.parse(text) : null;
  } catch {
    json = null;
  }
  const res = { status: r.status, ms: Date.now() - started, json, text: json === null ? text.slice(0, 400) : undefined };
  if (!r.ok && !ok.includes(r.status)) {
    const e = new Error(`${method} ${url} -> ${r.status} ${text.slice(0, 300)}`);
    e.res = res;
    throw e;
  }
  return res;
}

let MCP_TOKEN = "";
let mcpSeq = 0;
async function mcp(kb, name, args) {
  const r = await http("POST", `${API}/kbs/${kb}/mcp`, {
    bearer: MCP_TOKEN,
    body: { jsonrpc: "2.0", id: ++mcpSeq, method: "tools/call", params: { name, arguments: args } },
  });
  if (r.json?.error) throw new Error(`mcp ${name}: ${JSON.stringify(r.json.error)}`);
  const result = r.json?.result;
  if (result?.isError) throw new Error(`mcp ${name} tool error: ${result.content?.[0]?.text}`);
  return result?.structuredContent ?? null;
}

// 口令只进子进程环境，从不进命令行和日志
const dbUrl = process.env.REPLAY_DATABASE_URL ? new URL(process.env.REPLAY_DATABASE_URL) : null;
const pgEnv = dbUrl
  ? {
      PATH: process.env.PATH,
      PGHOST: dbUrl.hostname,
      PGPORT: dbUrl.port || "5432",
      PGUSER: decodeURIComponent(dbUrl.username),
      PGPASSWORD: decodeURIComponent(dbUrl.password),
      PGDATABASE: dbUrl.pathname.slice(1),
    }
  : null;
function sql(q) {
  if (!pgEnv) throw new Error("REPLAY_DATABASE_URL is needed for diagnostics and the fixture");
  return execFileSync("psql", ["-X", "-q", "-t", "-A", "-v", "ON_ERROR_STOP=1", "-c", q], {
    env: pgEnv,
    encoding: "utf8",
  }).trim();
}
function sqlRows(q) {
  return JSON.parse(sql(`SELECT coalesce(json_agg(t), '[]'::json) FROM (${q}) t`) || "[]");
}
const lit = (s) => `'${String(s).replaceAll("'", "''")}'`;
const dbNow = () => sql("SELECT to_json(clock_timestamp())#>>'{}'");

/** 等一个可观测的终态：有限超时，按固定间隔重读；超时导出诊断再抛出。成败只看读到的状态 */
async function until(label, fn, { timeoutMs = 60000, everyMs = 100, kb } = {}) {
  const started = Date.now();
  let last;
  while (Date.now() - started < timeoutMs) {
    last = await fn();
    if (last?.done) return { ...last, waitedMs: Date.now() - started };
    await sleep(everyMs);
  }
  const diag = kb ? diagnostics(kb) : null;
  rec("timeout", { label, timeoutMs, last, diag });
  throw new Error(`timed out after ${timeoutMs} ms: ${label}`);
}

function diagnostics(kb) {
  try {
    return {
      jobs: sqlRows(`SELECT id, kind, status, attempts, left(coalesce(last_error,''), 160) AS last_error, updated_at
                       FROM jobs WHERE payload->>'kb_id' = ${lit(kb)}
                          OR payload->>'document_id' IN (SELECT id::text FROM documents WHERE kb_id = ${lit(kb)})
                      ORDER BY id DESC LIMIT 30`),
      documents: sqlRows(`SELECT external_key, status, graph_status, left(coalesce(graph_error,''),160) AS graph_error, doc_time
                            FROM documents WHERE kb_id = ${lit(kb)} ORDER BY created_at`),
    };
  } catch (e) {
    return { error: String(e) };
  }
}

// ---------------------------------------------------------------------------
// 固定日志 → 推送载荷
// ---------------------------------------------------------------------------

const EV = Object.fromEntries(LOG.events.map((e) => [e.id, e]));

/** 一个事件的契约载荷（不含信封）：`e` 里是那样东西和它的类别词，`s` 一条陈述，引文空着 */
function contract(ev) {
  const src = ev.repeat ? EV[ev.repeat] : ev;
  const who = LOG.entities[src.subject];
  return {
    e: [[who.name, who.kind, true]],
    s: [[null, who.name, src.phrase, null, src.value, src.qualifiers ?? {}, src.observed_at, null]],
    n: [],
  };
}

// ---------------------------------------------------------------------------
// 一个库：本体、规则、来源、对齐
// ---------------------------------------------------------------------------

async function createKb(workspace, name, { intervalMinutes }) {
  const kb = (await http("POST", `${API}/workspaces/${workspace}/kbs`, { body: { name } })).json;
  // 推导开关保持新库的缺省（开着），删除、撤销这些路径才按生产的样子顺手重推。周期只影响
  // 定时任务：主场景设成最长，免得它在两站之间插一轮；调度器那一段设成最短，单独量
  await http("PATCH", `${API}/kbs/${kb.id}`, {
    body: { materialize_inferences: true, inference_interval_minutes: intervalMinutes },
  });
  const st = {
    id: kb.id,
    name,
    classes: {},
    rules: {},
    pushes: [],
    decisions: [],
    stages: [],
    interventions: [],
    bindings: {},
  };
  for (const [key, label] of [
    ["cup", "Cup"],
    ["box", "Box"],
    ["step_sa_ready", "S_A precondition holds"],
    ["step_sb_ready", "S_B precondition holds"],
  ]) {
    st.classes[key] = (await http("POST", `${API}/kbs/${kb.id}/ontology/entity-types`, { body: { key, label } })).json.id;
  }
  // 一个东西同一时刻只在一处：location 声明 functional
  st.location = (
    await http("POST", `${API}/kbs/${kb.id}/ontology/relation-types`, {
      body: {
        key: "location",
        label: "location",
        kind: "attribute",
        datatype: "text",
        temporal: "state",
        functional: true,
        domains: [st.classes.cup, st.classes.box],
      },
    })
  ).json.id;
  for (const [step, def] of Object.entries(LOG.steps)) {
    const subjectClass = LOG.entities[def.subject].kind;
    st.rules[step] = (
      await http("POST", `${API}/kbs/${kb.id}/rules`, {
        body: {
          name: def.rule,
          description: "The modelled precondition of a plan step; it says nothing about safety or success.",
          subject_type_id: st.classes[subjectClass],
          conclusion: "typing",
          conclude_type_id: st.classes[def.class],
          conditions: [{ group: 0, predicate_id: st.location, op: "in", operand: [def.place] }],
        },
      })
    ).json.id;
  }
  const src = (await http("POST", `${API}/kbs/${kb.id}/sources`, { body: { kind: "statements", name: "robot-1" } })).json;
  st.source = src.source.id;
  st.ingestToken = src.ingest_token;
  rec("kb.created", { kb: st.id, name, classes: st.classes, location: st.location, rules: st.rules, source: st.source, intervalMinutes });
  return st;
}

/** 新库第一次到点（从没推过的算到期）由调度器在一分钟内推一轮。主场景等它跑完再开始 */
async function awaitFirstScheduledRun(st) {
  const w = await until(
    "first scheduled derivation",
    async () => ({ done: sql(`SELECT last_inference_at IS NOT NULL FROM knowledge_bases WHERE id = ${lit(st.id)}`) === "t" }),
    { timeoutMs: 120000, everyMs: 500, kb: st.id },
  );
  rec("scheduler.first_run", { kb: st.id, waitedMs: w.waitedMs });
}

/** 等某种库级任务至少被试过一次、并因为没有对话模型而没做成：缺口的证据要是真跑过的任务 */
async function awaitModelless(st, kind) {
  const w = await until(
    `${kind} attempted without a chat model`,
    async () => {
      const rows = sqlRows(`SELECT id, status, attempts, last_error FROM jobs
                             WHERE kind = ${lit(kind)} AND payload->>'kb_id' = ${lit(st.id)} ORDER BY id DESC LIMIT 1`);
      const j = rows[0];
      return { done: !!j && j.attempts >= 1 && /Chat model not configured/.test(j.last_error || ""), j };
    },
    { timeoutMs: 120000, everyMs: 500, kb: st.id },
  );
  rec("gap.modelless_job", { kb: st.id, kind, job: w.j, waitedMs: w.waitedMs });
  return w.j;
}

/** 推一次，等它处理完（有限超时）。返回动作、文档、各段耗时 */
async function push(st, { external_id, doc_time, body, deleted, eventId, entity, observedAt }) {
  const envelope = deleted ? { external_id, deleted: true } : { external_id, doc_time, ...body };
  const sent = Date.now();
  const r = await http("POST", `${API}/sources/${st.source}/statements`, { bearer: st.ingestToken, body: envelope });
  const ackMs = Date.now() - sent;
  const action = r.json?.action;
  const p = { eventId, external_id, action, entity, observedAt, ackMs, sentMs: sent - T0 };
  if (action === "created" || action === "updated" || action === "moved") {
    const page = (
      await http("GET", `${API}/kbs/${st.id}/documents?source=${st.source}&q=${encodeURIComponent(external_id)}&limit=50`)
    ).json;
    const doc = page.docs.find((d) => d.external_key === `statements:${external_id}`);
    p.document = doc?.id;
  } else {
    const prior = [...st.pushes].reverse().find((x) => x.external_id === external_id && x.document);
    p.document = prior?.document;
  }
  if ((action === "created" || action === "updated") && p.document) {
    const w = await until(
      `document ${external_id} processed`,
      async () => {
        const d = (await http("GET", `${API}/documents/${p.document}`)).json.document;
        return { done: d.status === "failed" || (d.status === "ready" && ["done", "failed"].includes(d.graph_status)), d };
      },
      { timeoutMs: 60000, kb: st.id },
    );
    p.processedMs = Date.now() - sent;
    p.status = w.d.status;
    p.graphStatus = w.d.graph_status;
    p.graphError = w.d.graph_error;
  }
  if (p.document) {
    const d = (await http("GET", `${API}/documents/${p.document}`)).json.document;
    p.docTime = d.doc_time;
    p.externalKey = d.external_key;
    p.missingSince = d.missing_since;
  }
  st.pushes.push(p);
  rec("push", { kb: st.id, ...p });
  say(`  push ${eventId} ${external_id}: ${action}${p.processedMs ? ` (processed in ${p.processedMs} ms)` : ""}`);
  return p;
}

async function decideKindWord(st, word, cls) {
  const r = await http("POST", `${API}/kbs/${st.id}/review/alignment/kind-words/${encodeURIComponent(word)}`, {
    body: { class: cls },
  });
  st.interventions.push({ kind: "person", what: `kind word ${word} -> class ${cls}`, via: "POST review/alignment/kind-words" });
  rec("intervention.person", { kb: st.id, what: "kind_word", word, cls, status: r.status });
}

function alignmentQueue(st) {
  return http("GET", `${API}/kbs/${st.id}/review?queue=alignment&limit=200`).then((r) => r.json.items);
}

/**
 * 夹具：没有对话模型时对齐器从不写 `phrase_bindings`，Review 的对齐队列里也就没有短语签名可点。
 * 这里只替对齐器写下「两票不一致、留给人」的那一行（status = undecided、decided_by = agent），
 * 不写任何结论；绑到哪个属性由下面的人经公开接口定
 */
function fixtureUndecidedBinding(st, phrase) {
  const inserted = sql(`
    INSERT INTO phrase_bindings (id, kb_id, phrase, subject_type_id, object_type_id, object_is_value,
                                 relation_type_id, direction, status, votes, statement_count, examples, decided_by)
    SELECT gen_random_uuid(), ${lit(st.id)}, ${lit(phrase)}, t.id, NULL, true, NULL, NULL, 'undecided',
           '{"fixture": "premise-replay: no chat model is configured, so no aligner proposal exists; this row only stands in for an undecided proposal"}'::jsonb,
           0, '{}', 'agent'
      FROM entity_types t
     WHERE t.kb_id = ${lit(st.id)} AND t.key IN ('cup', 'box')
    ON CONFLICT DO NOTHING
    RETURNING id`);
  const ids = inserted ? inserted.split("\n").filter(Boolean) : [];
  st.interventions.push({ kind: "fixture", what: `undecided phrase_bindings rows for "${phrase}" x {cup, box} x value`, via: "SQL INSERT", rows: ids.length });
  rec("intervention.fixture", { kb: st.id, what: "undecided_phrase_binding", phrase, rows: ids });
  return ids;
}

/** 人定（或重定）一条短语签名 → 202 + job → 等 job 终态。这是无模型时让新陈述进类型化图谱的唯一公开入口 */
async function decidePhrase(st, bindingId, why) {
  const started = Date.now();
  const r = await http("POST", `${API}/kbs/${st.id}/review/alignment/phrases/${bindingId}`, {
    body: { property: "location", direction: "forward" },
  });
  const job = r.json.job_id;
  const w = await until(
    `materialize job ${job}`,
    async () => {
      const j = (await http("GET", `${API}/kbs/${st.id}/jobs/${job}`)).json.job;
      return { done: ["done", "failed"].includes(j.status), j };
    },
    { timeoutMs: 60000, kb: st.id },
  );
  const out = { job, status: w.j.status, attempts: w.j.attempts, last_error: w.j.last_error, ms: Date.now() - started };
  st.interventions.push({ kind: "person", what: `phrase binding ${bindingId} -> location (${why})`, via: "POST review/alignment/phrases", job: out });
  rec("intervention.person", { kb: st.id, what: "phrase_binding", why, binding: bindingId, ...out });
  return out;
}

async function reproject(st, why) {
  const out = [];
  for (const id of Object.values(st.bindings)) out.push(await decidePhrase(st, id, why));
  return out;
}

async function reconcile(st) {
  const r = await http("POST", `${API}/kbs/${st.id}/ontology/relation-types/${st.location}/reconcile`);
  st.interventions.push({ kind: "person", what: "reconcile location timelines", via: "POST ontology/relation-types/{id}/reconcile", result: r.json });
  rec("intervention.person", { kb: st.id, what: "reconcile", ms: r.ms, ...r.json });
  return { ...r.json, ms: r.ms };
}

async function derive(st) {
  const r = await http("POST", `${API}/kbs/${st.id}/rules/run`);
  st.interventions.push({ kind: "person", what: "run rules now", via: "POST rules/run", result: r.json });
  rec("intervention.person", { kb: st.id, what: "rules_run", ms: r.ms, ...r.json });
  return { ...r.json, ms: r.ms };
}

// ---------------------------------------------------------------------------
// 权威状态
// ---------------------------------------------------------------------------

async function entityId(st, name) {
  st.entityIds ??= {};
  if (st.entityIds[name]) return st.entityIds[name];
  const r = (await http("GET", `${API}/kbs/${st.id}/entities?q=${encodeURIComponent(name)}&limit=20`)).json;
  const exact = r.entities.filter((e) => e.name === name || e.canonical_name === name);
  if (exact.length !== 1) {
    rec("entity.lookup", { kb: st.id, name, matches: r.entities.length, exact: exact.length });
    if (exact.length === 0) return null;
  }
  st.entityIds[name] = exact[0].id;
  return exact[0].id;
}

// 按时刻比，不按字符串比：带小数秒的时间戳与不带的，字典序和先后不一致
const holds = (from, to, now) =>
  (!from || Date.parse(from) <= Date.parse(now)) && (!to || Date.parse(to) > Date.parse(now));
const valueOf = (v) => (v && typeof v === "object" ? v.value ?? v.class ?? JSON.stringify(v) : v);

/** 一个步骤此刻的样子：结论（MCP，世界轴 at = 回放时钟）、证明（REST）、这样东西的 location 行 */
async function stepState(st, step, now, asOf) {
  const def = LOG.steps[step];
  const name = LOG.entities[def.subject].name;
  const id = await entityId(st, name);
  if (!id) return { entity: null, holds: false, conclusions: [], locations: [] };
  const args = { entity_id: id, at: now };
  if (asOf) args.as_of = asOf;
  const snap = await mcp(st.id, "entity_facts", args);
  const conclusions = (snap?.derived_facts ?? []).filter((d) => d.attribute_rule_id === st.rules[step]);
  const detail = (await http("GET", `${API}/kbs/${st.id}/entities/${id}`)).json;
  const locations = detail.facts
    .filter((f) => f.predicate_key === "location")
    .map((f) => ({
      id: f.id,
      value: valueOf(f.object_value),
      holds_from: f.holds_from,
      holds_to: f.holds_to,
      valid_to_precision: f.valid_to_precision,
      stale: f.stale,
      last_evidence_time: f.last_evidence_time,
      documents: f.document_ids,
      supersedes: f.supersedes,
      contested: f.contested,
      holds_now: holds(f.holds_from, f.holds_to, now),
    }));
  // location 之外的读数（遮挡那条「看不见」在这里：它没有绑定，留在开放图谱）
  const other = detail.facts
    .filter((f) => f.predicate_key !== "location")
    .map((f) => ({ id: f.id, predicate: f.predicate_label, value: valueOf(f.object_value), stale: f.stale }));
  let proof = null;
  if (conclusions[0]) {
    const p = (await http("GET", `${API}/kbs/${st.id}/derived/${conclusions[0].id}/proof`)).json.proof;
    proof = p && {
      derived: p.derived?.id,
      premises: (p.steps ?? []).map((s) => ({
        fact: s.fact_id,
        retracted: s.retracted,
        object: s.object,
        evidence: (s.evidence ?? []).map((e) => ({ document: e.document_id, chunk: e.chunk_id, stale: e.stale, deleted: e.document_deleted, version: e.doc_version })),
      })),
    };
  }
  return {
    entity: id,
    holds: conclusions.length > 0,
    conclusions: conclusions.map((d) => ({ id: d.id, valid_from: d.valid_from, valid_to: d.valid_to, derived_at: d.derived_at })),
    proof,
    locations,
    other,
  };
}

async function queues(st) {
  const counts = (await http("GET", `${API}/kbs/${st.id}/review?queue=conflicts&limit=50`)).json;
  const stale = (await http("GET", `${API}/kbs/${st.id}/review?queue=unconfirmed&limit=50`)).json.items;
  return {
    conflicts: counts.items.map((c) => ({ id: c.id, reason: c.reason, old_fact: c.old_fact_id, old_object: c.old_object, new_fact: c.new_fact_id, new_object: c.new_object })),
    stale: stale.map((f) => ({ id: f.id, subject: f.subject_name, predicate: f.predicate_label, object: f.object_name })),
  };
}

/** 事件 → 文档（版本）→ 开放陈述 → 类型化事实 → 前提 → 结论，一次查库拼出来（诊断，不是适配器的读法） */
function lineage(st) {
  return sqlRows(`
    SELECT ar.name AS rule, df.id AS derived, df.valid_from AS d_from, df.valid_to AS d_to,
           df.invalidated_at AS d_invalidated, t.id AS typed, t.object_value->>'value' AS typed_value,
           t.valid_from AS t_from, t.valid_to AS t_to, t.valid_to_precision AS t_to_p, t.supersedes AS t_supersedes,
           t.invalidated_at AS t_invalidated,
           (SELECT json_agg(json_build_object('statement', s.id, 'phrase', s.phrase, 'value', s.object_value->>'value',
                                              'invalidated', s.invalidated_at IS NOT NULL,
                                              'documents', (SELECT json_agg(DISTINCT d.external_key) FROM fact_evidence fe
                                                              JOIN documents d ON d.id = fe.document_id WHERE fe.fact_id = s.id)))
              FROM typed_fact_sources ts JOIN facts s ON s.id = ts.statement_id WHERE ts.fact_id = t.id) AS sources,
           (SELECT json_agg(DISTINCT d.external_key) FROM fact_evidence fe JOIN documents d ON d.id = fe.document_id
             WHERE fe.fact_id = t.id) AS typed_evidence_documents
      FROM derived_facts df
      JOIN attribute_rules ar ON ar.id = df.attribute_rule_id
      JOIN fact_derivations fd ON fd.derived_fact_id = df.id
      JOIN facts t ON t.id = fd.premise_fact_id
     WHERE df.kb_id = ${lit(st.id)}
     ORDER BY df.derived_at, df.id`);
}

// ---------------------------------------------------------------------------
// 最小读取适配器
// ---------------------------------------------------------------------------

/**
 * SSE 只当「该重读了」的提示：连上、断了重连、每次（重）连上先整体重读一遍。决定只从重读到的
 * 权威状态里算，从不依赖通知的内容——通知里本来也没有事实。
 *
 * 决定是确定性的三种：continue / pause_reobserve / wait_confirmation，附理由。它不执行动作；
 * 这里也没有一致快照或版本校验可用，所以「检查完到执行前」之间状态仍可能变——这一点记在输出里。
 */
class Adapter {
  constructor(st) {
    this.st = st;
    this.plan = {};
    this.events = [];
    this.abort = null;
    this.connected = false;
    this.connects = 0;
    this.maxAgeMs = LOG.freshness.max_evidence_age_minutes * 60000;
  }

  async connect() {
    this.abort = new AbortController();
    const res = await fetch(`${BASE}${API}/kbs/${this.st.id}/events`, {
      headers: { authorization: `Bearer ${JWT}`, accept: "text/event-stream" },
      signal: this.abort.signal,
    });
    if (!res.ok) throw new Error(`sse ${res.status}`);
    this.connected = true;
    this.connects += 1;
    rec("sse.connected", { kb: this.st.id, connects: this.connects });
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    (async () => {
      let buf = "";
      try {
        for (;;) {
          const { value, done } = await reader.read();
          if (done) break;
          buf += decoder.decode(value, { stream: true });
          let cut;
          while ((cut = buf.indexOf("\n\n")) >= 0) {
            const frame = buf.slice(0, cut);
            buf = buf.slice(cut + 2);
            const kind = frame.match(/^event: ?(.*)$/m)?.[1];
            const data = frame.match(/^data: ?(.*)$/m)?.[1];
            if (!kind) continue;
            const ev = { ms: Date.now() - T0, kind, data };
            this.events.push(ev);
            rec("sse.event", { kb: this.st.id, kind, data });
          }
        }
      } catch (e) {
        if (e.name !== "AbortError") rec("sse.error", { kb: this.st.id, error: String(e) });
      } finally {
        this.connected = false;
        rec("sse.closed", { kb: this.st.id });
      }
    })();
  }

  disconnect() {
    this.abort?.abort();
  }

  /** 重连直到连上（有限次数），连上立刻整体重读：通知不补发，丢了什么只能靠重读发现 */
  async reconnect(now) {
    for (let i = 0; i < 60; i++) {
      try {
        await this.connect();
        break;
      } catch (e) {
        await sleep(500);
      }
    }
    if (!this.connected) throw new Error("sse did not come back");
    return this.resync(now, "reconnect");
  }

  /** 等下一条某种事件（只用于量「适配器何时看到变化」，成败不看它） */
  async nextEvent(kind, sinceMs, timeoutMs = 10000) {
    const started = Date.now();
    while (Date.now() - started < timeoutMs) {
      const hit = this.events.find((e) => e.kind === kind && e.ms >= sinceMs);
      if (hit) return hit;
      await sleep(20);
    }
    return null;
  }

  /** 计划时刻记下每个步骤的结论与前提引用 */
  async recordPlan(now) {
    for (const step of Object.keys(LOG.steps)) {
      const s = await stepState(this.st, step, now);
      this.plan[step] = s.proof ? { derived: s.proof.derived, premises: s.proof.premises.map((p) => p.fact).sort() } : null;
    }
    rec("adapter.plan", { kb: this.st.id, plan: this.plan });
  }

  async resync(now, why) {
    const out = {};
    for (const step of Object.keys(LOG.steps)) out[step] = await this.decide(step, now);
    rec("adapter.resync", { kb: this.st.id, why, now, decisions: out });
    return out;
  }

  async decide(step, now) {
    const def = LOG.steps[step];
    const who = def.subject;
    let s;
    let q;
    try {
      s = await stepState(this.st, step, now);
      q = await queues(this.st);
    } catch (e) {
      return { decision: "wait_confirmation", reason: `state unreadable: ${e.message}` };
    }
    // 1. 推过的观察还没处理完：看不见它的结果之前不下结论
    const mine = this.st.pushes.filter((p) => p.entity === who);
    for (const p of mine) {
      if (!p.document || !["created", "updated"].includes(p.action)) continue;
      const d = (await http("GET", `${API}/documents/${p.document}`)).json.document;
      if (d.deleted_at) continue;
      if (!(d.status === "ready" && d.graph_status === "done")) {
        return { decision: "wait_confirmation", reason: `observation ${p.external_id} accepted but not processed (${d.status}/${d.graph_status})`, s };
      }
    }
    // 2. 这一步的前提现在不成立
    if (!s.holds) return { decision: "pause_reobserve", reason: "the precondition does not hold now in the ledger", s };
    if (!s.proof) return { decision: "wait_confirmation", reason: "the conclusion holds but its proof could not be read", s };
    const premiseFacts = new Set(s.proof.premises.map((p) => p.fact));
    // 3. 结论还在，前提已经撤了：推导还没重跑
    if (s.proof.premises.some((p) => p.retracted)) {
      return { decision: "pause_reobserve", reason: "a premise was retracted and the conclusion has not been re-derived yet", s };
    }
    // 4. 前提的证据只剩旧版本（同一身份推了新版本，旧陈述 stale 却没作废）
    const staleOnly = s.proof.premises.some((p) => p.evidence.length > 0 && p.evidence.every((e) => e.stale || e.deleted));
    if (staleOnly) return { decision: "pause_reobserve", reason: "the premise is only supported by superseded versions (stale, not invalidated)", s };
    // 5. 我们推过的最新一次位置观察还没在类型化事实里（投影没跟上）。被墓碑撤回的观察不算
    const withdrawn = new Set(mine.filter((p) => p.action === "marked_missing").map((p) => p.external_id));
    const newestObserved = mine
      .filter((p) => p.observedAt && !withdrawn.has(p.external_id) && EV[p.eventId]?.phrase === def.phrase)
      .map((p) => iso(p.observedAt))
      .sort()
      .pop();
    const newestInLedger = s.locations
      .map((l) => l.last_evidence_time && iso(l.last_evidence_time))
      .filter(Boolean)
      .sort()
      .pop();
    if (newestObserved && (!newestInLedger || newestInLedger < newestObserved)) {
      return { decision: "wait_confirmation", reason: `latest observed location (${newestObserved}) is not reflected in typed facts (newest ${newestInLedger})`, s };
    }
    // 6. functional 属性上此刻还有另一个值成立：账本没把旧值关上
    const rivals = s.locations.filter((l) => l.holds_now && l.value !== def.place);
    if (rivals.length) {
      return { decision: "wait_confirmation", reason: `another current ${def.attribute} holds (${rivals.map((r) => r.value).join(", ")}); the earlier value was not closed`, s };
    }
    // 7. 有等人裁的时态冲突挂在这些行上
    const involved = new Set([...premiseFacts, ...s.locations.map((l) => l.id)]);
    const conflict = q.conflicts.find((c) => involved.has(c.old_fact) || involved.has(c.new_fact));
    if (conflict) return { decision: "wait_confirmation", reason: `temporal conflict ${conflict.reason} awaits a person`, s };
    // 8. 新鲜度（适配器自己的策略，不是账本的 TTL）
    const premiseEvidence = s.locations
      .filter((l) => premiseFacts.has(l.id))
      .map((l) => l.last_evidence_time && Date.parse(l.last_evidence_time))
      .filter(Boolean);
    const newest = premiseEvidence.length ? Math.max(...premiseEvidence) : null;
    if (newest === null || Date.parse(now) - newest > this.maxAgeMs) {
      return { decision: "pause_reobserve", reason: `premise evidence older than the ${LOG.freshness.max_evidence_age_minutes}-minute freshness policy`, s };
    }
    // 9. 计划之后前提引用变了：结论仍成立，但依据换了，交给计划器确认
    const planned = this.plan[step];
    const current = s.proof ? { derived: s.proof.derived, premises: s.proof.premises.map((p) => p.fact).sort() } : null;
    if (planned && current && (planned.derived !== current.derived || JSON.stringify(planned.premises) !== JSON.stringify(current.premises))) {
      return { decision: "wait_confirmation", reason: "the conclusion or its premise references changed since planning", s };
    }
    return { decision: "continue", reason: "precondition holds now, premises live, fresh and unchanged", s };
  }
}

// ---------------------------------------------------------------------------
// 一站一站地量
// ---------------------------------------------------------------------------

async function stage(st, adapter, eventId, name, now, extra = {}) {
  const decisions = {};
  const states = {};
  for (const step of Object.keys(LOG.steps)) {
    const d = await adapter.decide(step, now);
    decisions[step] = { decision: d.decision, reason: d.reason };
    states[step] = d.s
      ? { holds: d.s.holds, conclusions: d.s.conclusions, premises: d.s.proof?.premises, locations: d.s.locations, other: d.s.other }
      : null;
  }
  const q = await queues(st);
  const row = { kb: st.name, event: eventId, stage: name, now, decisions, states, conflicts: q.conflicts, stale: q.stale, ...extra };
  st.stages.push(row);
  rec("stage", row);
  const brief = Object.entries(decisions)
    .map(([k, v]) => `${k}:${states[k]?.holds ? "holds" : "no"}→${v.decision}`)
    .join("  ");
  say(`    ${eventId} ${name.padEnd(18)} ${brief}`);
  return row;
}

async function bootstrapAlignment(st) {
  // 类别词对齐器（抽取完排的）真的跑过、因为没有模型没做成
  const alignTypes = await awaitModelless(st, "align_types");
  await decideKindWord(st, "cup", "cup");
  await decideKindWord(st, "box", "box");
  // 人定了类别词，短语对齐器被排上、同样没做成；公开的对齐队列里也就没有短语签名可点
  const alignPhrases = await awaitModelless(st, "align_phrases");
  const before = (await alignmentQueue(st)).filter((i) => i.kind === "phrase");
  const bindingRows = sql(`SELECT count(*) FROM phrase_bindings WHERE kb_id = ${lit(st.id)}`);
  rec("gap.alignment_queue_empty", { kb: st.id, phraseItems: before.length, bindingRows });
  st.gapEvidence = {
    phraseItemsBeforeFixture: before.length,
    phraseBindingRowsBeforeFixture: Number(bindingRows),
    alignTypes: { attempts: alignTypes.attempts, status: alignTypes.status, last_error: alignTypes.last_error },
    alignPhrases: { attempts: alignPhrases.attempts, status: alignPhrases.status, last_error: alignPhrases.last_error },
  };
  fixtureUndecidedBinding(st, "is on");
  const items = (await alignmentQueue(st)).filter((i) => i.kind === "phrase" && i.phrase === "is on");
  for (const it of items) st.bindings[it.subject_class] = it.id;
  const first = await reproject(st, "first decision");
  return first;
}

/** 一种身份策略跑完整个日志 */
async function runStrategy(workspace, strategy) {
  say(`strategy ${strategy}`);
  const st = await createKb(workspace, `premise-replay ${strategy}`, { intervalMinutes: 10080 });
  st.strategy = strategy;
  await awaitFirstScheduledRun(st);
  const adapter = new Adapter(st);
  await adapter.connect();
  const idOf = (ev, subject) => (strategy === "per_object" ? LOG.entities[subject].name : `obs-${ev.id}`);
  const current = {}; // per_object：每样东西当前那份载荷

  const pushEvent = async (ev, overrides = {}) => {
    const subject = ev.subject ?? EV[ev.repeat].subject;
    const src = ev.repeat ? EV[ev.repeat] : ev;
    const body = overrides.body ?? contract(ev);
    const external_id = overrides.external_id ?? idOf(ev.repeat ? EV[ev.repeat] : ev, subject);
    const doc_time = overrides.doc_time ?? src.observed_at;
    const p = await push(st, { external_id, doc_time, body, eventId: ev.id, entity: subject, observedAt: overrides.observedAt ?? src.observed_at, deleted: overrides.deleted });
    if (!overrides.deleted) current[subject] = { body, observed_at: src.observed_at };
    return p;
  };

  // ---- E1 初始
  const e1 = [EV["E1-A"], EV["E1-B"]];
  for (const ev of e1) await pushEvent(ev);
  let now = EV["E1-A"].arrive_at;
  await stage(st, adapter, "E1", "after_push", now);
  await bootstrapAlignment(st);
  await stage(st, adapter, "E1", "after_projection", now);
  await derive(st);
  await stage(st, adapter, "E1", "after_derive", now);
  await adapter.recordPlan(now);
  st.lineageInitial = lineage(st);
  await stage(st, adapter, "E1", "planned", now);

  // ---- E2 同一份载荷再推一遍
  now = EV["E2-A"].arrive_at;
  const dup = await pushEvent(EV["E2-A"]);
  const docs = sqlRows(`SELECT count(*)::int AS documents,
                               (SELECT count(*)::int FROM document_versions v JOIN documents d ON d.id = v.document_id WHERE d.kb_id = ${lit(st.id)}) AS versions,
                               (SELECT count(*)::int FROM facts WHERE kb_id = ${lit(st.id)} AND layer = 'open' AND invalidated_at IS NULL) AS open_statements
                          FROM documents WHERE kb_id = ${lit(st.id)} AND deleted_at IS NULL`)[0];
  await stage(st, adapter, "E2", "after_push", now, { action: dup.action, counts: docs });
  const r2 = await derive(st);
  await stage(st, adapter, "E2", "after_derive", now, { derive: r2 });

  // ---- E3 遮挡：一条「看不见」的陈述，没有 location
  now = EV["E3-A"].arrive_at;
  await pushEvent(EV["E3-A"]);
  await stage(st, adapter, "E3", "after_push", now);
  await reproject(st, "after occlusion");
  await derive(st);
  await stage(st, adapter, "E3", "after_derive", now);

  // ---- E4 移动。SSE 在这之前断开：这一段的通知全部丢失，重连之后只能靠重读发现
  adapter.disconnect();
  rec("sse.dropped_on_purpose", { kb: st.id, before: "E4" });
  now = EV["E4-A"].arrive_at;
  const moved = await pushEvent(EV["E4-A"]);
  await stage(st, adapter, "E4", "after_push", now);
  const proj = await reproject(st, "after move");
  await stage(st, adapter, "E4", "after_projection", now);
  const d4a = await derive(st);
  await stage(st, adapter, "E4", "after_derive", now, { derive: d4a });
  const beforeReconcile = dbNow();
  const rc = await reconcile(st);
  await stage(st, adapter, "E4", "after_reconcile", now, { reconcile: rc });
  const d4b = await derive(st);
  await stage(st, adapter, "E4", "after_reconcile_derive", now, { derive: d4b });
  if (strategy === "per_object") {
    // 对账关不上（两行同一刻）：人把桌面那一行关在移动那一刻
    const s = await stepState(st, "S_A", now);
    const desk = s.locations.find((l) => l.value === "desk" && l.holds_now);
    if (desk) {
      const r = await http("POST", `${API}/kbs/${st.id}/facts/${desk.id}/close`, {
        body: { valid_to: EV["E4-A"].observed_at, valid_to_precision: "second" },
      });
      st.interventions.push({ kind: "person", what: "close the desk row at the move's observed time", via: "POST facts/{id}/close" });
      rec("intervention.person", { kb: st.id, what: "close_fact", fact: desk.id, status: r.status });
      await stage(st, adapter, "E4", "after_human_close", now);
      const d4c = await derive(st);
      await stage(st, adapter, "E4", "after_human_close_derive", now, { derive: d4c });
    }
  }
  st.lineageAfterMove = lineage(st);
  // 当时所知 vs 现在回看：记录轴回到对账之前，库认为 t=移动之后 S_A 成立；现在回看，它在那时已不成立
  const probe = "2026-09-23T08:11:00Z";
  const thenKnown = await stepState(st, "S_A", probe, beforeReconcile);
  const nowKnown = await stepState(st, "S_A", probe);
  rec("two_axes", { kb: st.id, world_at: probe, record_as_of: beforeReconcile, then_holds: thenKnown.holds, now_holds: nowKnown.holds });
  st.twoAxes = { world_at: probe, record_as_of: beforeReconcile, then_holds: thenKnown.holds, now_holds: nowKnown.holds };
  // 重连：先整体重读，再比对计划时记下的引用
  const reconnectStarted = Date.now();
  const afterReconnect = await adapter.reconnect(now);
  st.reconnect = {
    ms: Date.now() - reconnectStarted,
    missedWhileDisconnected: "unknown by construction: the stream has no ids and no replay",
    decisions: Object.fromEntries(Object.entries(afterReconnect).map(([k, v]) => [k, { decision: v.decision, reason: v.reason }])),
  };
  await stage(st, adapter, "E4", "after_sse_reconnect", now);

  // ---- E5 晚到的旧观察（t0 早于 t1），在移动之后才送到
  now = EV["E5-A"].arrive_at;
  await pushEvent(EV["E5-A"]);
  await stage(st, adapter, "E5", "after_push", now);
  await reproject(st, "after late observation");
  await stage(st, adapter, "E5", "after_projection", now);
  await derive(st);
  await stage(st, adapter, "E5", "after_derive", now);
  await reconcile(st);
  await derive(st);
  await stage(st, adapter, "E5", "after_reconcile_derive", now);

  // ---- 服务重启：连接全断，任务在库里；适配器重连后整体重读
  if (RESTART_DIR && strategy === "per_observation") {
    const sinceMs = Date.now() - T0;
    fs.writeFileSync(path.join(RESTART_DIR, "request"), String(sinceMs));
    rec("restart.requested", { kb: st.id });
    await until("server restart", async () => ({ done: fs.existsSync(path.join(RESTART_DIR, "done")) }), { timeoutMs: 120000, everyMs: 250 });
    fs.rmSync(path.join(RESTART_DIR, "done"), { force: true });
    rec("restart.done", { kb: st.id });
    const decisions = await adapter.reconnect(now);
    st.restart = Object.fromEntries(Object.entries(decisions).map(([k, v]) => [k, { decision: v.decision, reason: v.reason }]));
    await stage(st, adapter, "E5", "after_server_restart", now);
  }

  // ---- E6 内容相同、doc_time 变了
  now = EV["E6-A"].arrive_at;
  const same = current.A;
  const e6 = [];
  if (strategy === "per_observation") {
    e6.push(await pushEvent(EV["E6-A"], { external_id: `obs-E4-A`, doc_time: EV["E6-A"].doc_time, body: contract(EV["E4-A"]), observedAt: null }));
    e6.push(await pushEvent(EV["E6-A"], { external_id: `obs-E6-A`, doc_time: EV["E6-A"].doc_time, body: contract(EV["E4-A"]), observedAt: null }));
  } else {
    e6.push(await pushEvent(EV["E6-A"], { external_id: "cup-7", doc_time: EV["E6-A"].doc_time, body: same.body, observedAt: null }));
  }
  const e6docs = sqlRows(`SELECT external_key, doc_time, (SELECT count(*)::int FROM document_versions v WHERE v.document_id = d.id) AS versions
                            FROM documents d WHERE kb_id = ${lit(st.id)} AND deleted_at IS NULL ORDER BY created_at`);
  await stage(st, adapter, "E6", "after_push", now, { actions: e6.map((p) => `${p.external_id}:${p.action}`), documents: e6docs });

  // ---- E7 明确结束 B 的那条观察：墓碑 → 清理
  now = EV["E7-B"].arrive_at;
  const bId = strategy === "per_object" ? LOG.entities.B.name : "obs-E1-B";
  const tomb = await pushEvent(EV["E7-B"], { external_id: bId, deleted: true, body: null, observedAt: null });
  await stage(st, adapter, "E7", "after_tombstone", now, { action: tomb.action });
  const cleaned = await http("POST", `${API}/kbs/${st.id}/sources/${st.source}/missing/cleanup`);
  st.interventions.push({ kind: "person", what: "clean up missing documents", via: "POST sources/{id}/missing/cleanup", result: cleaned.json });
  rec("intervention.person", { kb: st.id, what: "missing_cleanup", ...cleaned.json });
  await stage(st, adapter, "E7", "after_cleanup", now, { cleanup: cleaned.json });
  await derive(st);
  await stage(st, adapter, "E7", "after_cleanup_derive", now);

  st.lineageFinal = lineage(st);
  st.jobs = sqlRows(`SELECT kind, status, count(*)::int AS n, max(left(coalesce(last_error,''), 100)) AS example_error
                       FROM jobs WHERE payload->>'kb_id' = ${lit(st.id)}
                          OR payload->>'document_id' IN (SELECT id::text FROM documents WHERE kb_id = ${lit(st.id)})
                      GROUP BY kind, status ORDER BY kind, status`);
  st.sse = { connects: adapter.connects, events: adapter.events.length, kinds: [...new Set(adapter.events.map((e) => e.kind))] };
  adapter.disconnect();
  return st;
}

/** 调度器那一段：推导不显式调，等定时任务。只量一次，不当性能保证 */
async function runScheduler(workspace) {
  say("scheduler");
  const st = await createKb(workspace, "premise-replay scheduler", { intervalMinutes: 5 });
  st.strategy = "per_observation (scheduled derivation)";
  const push1 = await push(st, { external_id: "obs-E1-A", doc_time: EV["E1-A"].observed_at, body: contract(EV["E1-A"]), eventId: "E1-A", entity: "A", observedAt: EV["E1-A"].observed_at });
  await bootstrapAlignment(st);
  const projected = Date.now();
  const appear = await until(
    "scheduled derivation concludes S_A",
    async () => ({ done: (await stepState(st, "S_A", EV["E1-A"].arrive_at)).holds }),
    { timeoutMs: 7.5 * 60000, everyMs: 2000, kb: st.id },
  );
  const appearMs = Date.now() - projected;
  await push(st, { external_id: "obs-E4-A", doc_time: EV["E4-A"].observed_at, body: contract(EV["E4-A"]), eventId: "E4-A", entity: "A", observedAt: EV["E4-A"].observed_at });
  await reproject(st, "after move");
  await reconcile(st);
  const reconciled = Date.now();
  const leave = await until(
    "scheduled derivation retracts S_A",
    async () => ({ done: !(await stepState(st, "S_A", EV["E4-A"].arrive_at)).holds }),
    { timeoutMs: 7.5 * 60000, everyMs: 2000, kb: st.id },
  );
  const leaveMs = Date.now() - reconciled;
  st.scheduler = {
    interval_minutes: 5,
    scheduler_tick_seconds: 60,
    appear_after_projection_ms: appearMs,
    retract_after_reconcile_ms: leaveMs,
    runs: sqlRows(`SELECT id, status, created_at, updated_at FROM jobs WHERE kind = 'materialize_inferences' AND payload->>'kb_id' = ${lit(st.id)} ORDER BY id`),
    push_ack_ms: push1.ackMs,
  };
  rec("scheduler", { kb: st.id, ...st.scheduler });
  say(`  scheduled derivation: S_A appeared ${Math.round(appearMs / 1000)} s after projection, left ${Math.round(leaveMs / 1000)} s after reconcile`);
  return st;
}

// ---------------------------------------------------------------------------

async function main() {
  const health = await http("GET", `${API}/health`);
  rec("start", { base: BASE, health: health.json, strategies: ["per_observation", "per_object"], scheduler: SCHEDULER });
  const password = `pr-${Math.random().toString(36).slice(2)}-${Date.now().toString(36)}`;
  const reg = await http("POST", `${API}/auth/register`, {
    body: { email: "replay@premise-replay.test", password, display_name: "premise replay", org_name: "premise-replay" },
  });
  JWT = reg.json.token;
  const workspace = reg.json.workspace.id;
  MCP_TOKEN = (await http("POST", `${API}/me/tokens`, { body: { name: "premise-replay adapter", scope: "read", expires_in_days: 1 } })).json.token;
  if (!MCP_TOKEN) throw new Error("no personal token issued");

  const results = [];
  for (const strategy of ["per_observation", "per_object"]) results.push(await runStrategy(workspace, strategy));
  if (SCHEDULER) results.push(await runScheduler(workspace));

  const summary = results.map((st) => ({
    kb: st.id,
    name: st.name,
    strategy: st.strategy,
    interventions: st.interventions,
    gapEvidence: st.gapEvidence,
    pushes: st.pushes,
    stages: st.stages,
    twoAxes: st.twoAxes,
    reconnect: st.reconnect,
    restart: st.restart,
    lineage: { initial: st.lineageInitial, afterMove: st.lineageAfterMove, final: st.lineageFinal },
    jobs: st.jobs,
    sse: st.sse,
    scheduler: st.scheduler,
  }));
  fs.writeFileSync(path.join(OUT, "summary.json"), JSON.stringify(summary, null, 2));
  rec("end", {});
  say(`done: ${path.join(OUT, "summary.json")}`);
}

main()
  .catch((e) => {
    rec("fatal", { error: String(e.stack || e), res: e.res });
    console.error(e);
    process.exitCode = 1;
  })
  .finally(() => traceFile.end());
