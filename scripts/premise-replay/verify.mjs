import assert from "node:assert/strict";
import fs from "node:fs";
import { pathToFileURL } from "node:url";

// These assertions characterize this no-model experiment, including known gaps.
// A changed upstream contract must fail visibly and be reviewed, not be silently
// counted as a successful end-to-end decision loop.
export function verifySummary(summary, { requireLiveEvents = true } = {}) {
  let checks = 0;
  const check = (actual, expected, label) => {
    assert.deepEqual(actual, expected, label);
    checks++;
  };
  for (const strategy of ["per_observation", "per_object"]) {
    const run = summary.find((r) => r.strategy === strategy);
    assert.ok(run, `missing strategy ${strategy}`);
    const stage = (event, name) => {
      const rows = run.stages.filter((s) => s.event === event && s.stage === name);
      check(rows.length, 1, `${strategy} missing or repeated ${event}/${name}`);
      return rows[0];
    };
    const initial = stage("E1", "planned");
    for (const step of ["S_A", "S_B"]) {
      check(initial.states[step].holds, true, `${strategy} initial ${step}`);
      check(initial.decisions[step].decision, "continue", `${strategy} initial decision ${step}`);
      assert.ok(initial.states[step].premises.length, "initial conclusion must have premises");
    }
    const duplicate = stage("E2", "after_push");
    check(duplicate.action, "unchanged", "duplicate must not create a version");
    check(duplicate.counts, { documents: 2, versions: 2, open_statements: 2 }, "duplicate counts");
    const occlusion = stage("E3", "after_derive");
    check(occlusion.states.S_A.holds, true, "occlusion must not assert absence");
    check(occlusion.decisions.S_A.decision, strategy === "per_object" ? "pause_reobserve" : "continue", "occlusion policy");
    const before = stage("E4", "after_derive");
    check(before.states.S_A.holds, true, "known gap: projection does not reconcile");
    assert.notEqual(before.decisions.S_A.decision, "continue", "adapter must stop on the move");
    const reconciled = stage("E4", "after_reconcile_derive");
    check(reconciled.states.S_A.holds, strategy === "per_object", "identity-dependent reconciliation");
    if (strategy === "per_object") {
      check(stage("E4", "after_human_close_derive").states.S_A.holds, false, "explicit close");
    }
    for (const row of run.stages.filter((s) => ["E2", "E3", "E4", "E5"].includes(s.event))) {
      check(row.states.S_B.holds, true, "unrelated step must remain valid");
      check(row.states.S_B.conclusions.map((c) => c.id), initial.states.S_B.conclusions.map((c) => c.id), "unrelated conclusion identity");
    }
    check(stage("E4", "after_sse_reconnect").states.S_A.holds, false, "reconnect state");
    check(run.reconnect.decisions.S_A.decision, "pause_reobserve", "reconnect decision");
    check(run.twoAxes.then_holds, true, "record-axis history");
    check(run.twoAxes.now_holds, false, "current interpretation of past world time");
    const late = stage("E5", "after_projection");
    const desk = late.states.S_A.locations.find((l) => l.value === "desk");
    assert.ok(desk, "historical desk interval must remain");
    check(Date.parse(desk.holds_from), Date.parse("2026-09-23T07:50:00Z"), "late observation start");
    check(Date.parse(desk.holds_to), Date.parse("2026-09-23T08:10:00Z"), "late observation must preserve end");
    check(stage("E5", "after_derive").states.S_A.holds, false, "late observation must not revive step");
    if (run.restart) check(run.restart.S_A.decision, "pause_reobserve", "restart decision");
    const fresh = stage("E6", "after_push");
    check(fresh.actions.map((a) => a.split(":").pop()), strategy === "per_object" ? ["unchanged"] : ["unchanged", "moved"], "timestamp-only identity behavior");
    check(fresh.decisions.S_B.decision, "pause_reobserve", "freshness policy");
    check(stage("E7", "after_tombstone").states.S_B.holds, true, "tombstone alone is not deletion");
    check(stage("E7", "after_cleanup").states.S_B.holds, false, "cleanup must settle derivations immediately");
    check(stage("E7", "after_cleanup_derive").states.S_B.holds, false, "cleanup remains settled");
    if (requireLiveEvents) {
      assert.ok(run.liveRefresh?.events > 0, "must observe a live event after cleanup begins");
      check(run.liveRefresh.decisions.S_B.s.holds, false, "event callback must read the retraction without a manual stage");
      check(run.liveRefresh.decisions.S_B.decision, "pause_reobserve", "event-driven pause");
    }
  }
  return { checks, status: "passed", scope: "assisted no-model replay; known gaps explicitly asserted" };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const result = verifySummary(JSON.parse(fs.readFileSync(process.argv[2], "utf8")));
  console.log(JSON.stringify(result));
}
