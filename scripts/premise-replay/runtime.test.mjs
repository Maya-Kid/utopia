import assert from "node:assert/strict";
import { test } from "node:test";
import { createServer } from "node:http";
import { mkdtempSync, writeFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { execFileSync } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";
import { EventStream } from "./event-stream.mjs";

async function until(predicate) {
  for (let i = 0; i < 200; i++) {
    if (predicate()) return;
    await sleep(10);
  }
  assert.fail("timed out waiting for event-driven refresh");
}

async function streamServer(t) {
  let response;
  const server = createServer((req, res) => {
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.flushHeaders();
    response = res;
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  t.after(() => { server.closeAllConnections(); return new Promise((r) => server.close(r)); });
  return {
    url: `http://127.0.0.1:${server.address().port}/events`,
    write: (s) => response.write(s),
    end: () => response.end(),
  };
}

test("psql receives literal decoded credentials, including shell metacharacters", (t) => {
  const dir = mkdtempSync(path.join(tmpdir(), "premise-pg-env-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  const password = "p$HOME\`uname\`$(id)'\\\" \n";
  const user = "user$name";
  const db = "db name";
  writeFileSync(path.join(dir, "psql"), '#!/usr/bin/env node\nconsole.log(JSON.stringify([process.env.PGUSER, process.env.PGPASSWORD, process.env.PGDATABASE, process.argv.slice(2)]));\n', { mode: 0o700 });
  const result = execFileSync(process.execPath, [fileURLToPath(new URL("./pg-env.mjs", import.meta.url)), "-X", "-tAc", "SELECT 1"], {
    env: { ...process.env, PATH: dir + path.delimiter + process.env.PATH, REPLAY_DATABASE_URL: `postgres://${encodeURIComponent(user)}:${encodeURIComponent(password)}@localhost/${encodeURIComponent(db)}` },
    encoding: "utf8",
  });
  assert.deepEqual(JSON.parse(result), [user, password, db, ["-X", "-tAc", "SELECT 1"]]);
});

test("split SSE frames trigger serialized refreshes, including a change during a read", async (t) => {
  const server = await streamServer(t);
  let state = 0, seen = -1, reads = 0, active = 0, peak = 0, release;
  const gate = new Promise((r) => { release = r; });
  const events = [];
  const errors = [];
  const stream = new EventStream({
    refresh: async () => {
      reads++; active++; peak = Math.max(peak, active);
      const captured = state;
      if (captured === 1) await gate;
      seen = captured;
      active--;
    },
    event: (e) => events.push(e), error: (e) => errors.push(e), closed: () => {},
  });
  t.after(() => stream.disconnect());
  await stream.connect(server.url, {});
  assert.equal(seen, 0, "connection must refresh even without an event");
  state = 1;
  server.write("event: gra");
  server.write("ph\r\ndata: {}\r\n\r\n");
  await until(() => active === 1);
  state = 2;
  server.write("event: graph\ndata: {}\n\nevent: review\ndata: {}\n\n");
  await until(() => events.length === 3);
  release();
  await until(() => seen === 2);
  assert.equal(peak, 1, "event bursts must not overlap authoritative reads");
  assert.equal(reads, 3, "a hint during a read requires one follow-up read");
  assert.deepEqual(errors, []);
  await stream.disconnect();
});

test("reconnect drains an old read and recovers missed state without an event", async (t) => {
  const server = await streamServer(t);
  let state = 0, seen, release, started = false;
  const gate = new Promise((r) => { release = r; });
  const stream = new EventStream({
    refresh: async () => {
      const captured = state;
      if (captured === 1) { started = true; await gate; }
      seen = captured;
    },
    event: () => {}, error: () => {}, closed: () => {},
  });
  t.after(() => stream.disconnect());
  await stream.connect(server.url, {});
  state = 1;
  server.write("event: graph\ndata: {}\n\n");
  await until(() => started);
  server.end();
  state = 2;
  const reconnect = stream.connect(server.url, {});
  release();
  await reconnect;
  assert.equal(seen, 2, "the old read cannot overwrite the recovered state");
  await stream.disconnect();
});
