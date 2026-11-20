import assert from "node:assert/strict";
import test, { after, type TestContext } from "node:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import Database from "better-sqlite3";
import { canonicalJson, digest, type PcpSyncSelection } from "../src/matrix/pcp-sync-observation.js";
import * as boundaries from "../src/matrix/pcp-sync-handoff.js";

for (const name of Object.keys(process.env)) if (name.startsWith("MATRIX_")) delete process.env[name];
const moduleData = mkdtempSync(path.join(tmpdir(), "matrix-start-module-fixture-"));
process.env.MATRIX_DATA_DIR = moduleData;
const { MessageQueue } = await import("../src/matrix/messageQueue.js");
after(() => rmSync(moduleData, { recursive: true, force: true }));

const selection: PcpSyncSelection = {
  homeserverUrl: "https://matrix.example.com", userId: "@owner:example.com", deviceId: "FIXTURE",
  receiverUrl: "https://pcp.example.com/v1/source/matrix-discord",
  policy: { schema: "ea-chat-source-v1", source: "matrix", account: "@owner:example.com",
    room: "!selected:example.com", profile: "personal", principal: "service:fixture",
    policy_version: "fixture-v1", mode: "live", basis: "explicit-selection" },
};
const cursor = "opaque-existing-cursor/with-no-selected-custody";
const plan = {
  schema: "ea-matrix-pcp-start-v1" as const,
  selection_sha256: digest(canonicalJson(selection)), checkpoint_sha256: digest(cursor),
  decision_ref: "fixture:explicit-future-start", prior_coverage: "unverified" as const,
};
const sync = (next = "new-response") => Buffer.from(JSON.stringify({ next_batch: next, rooms: { join: {
  [selection.policy.room]: { timeline: { events: [{ event_id: "$new", sender: selection.userId,
    type: "m.room.message", origin_server_ts: 1_800_000_000_000,
    content: { msgtype: "m.text", body: "Fabricated future message" } }], limited: false } },
} } }));

function fixture(t: TestContext, token: string | null = cursor) {
  const root = mkdtempSync(path.join(tmpdir(), "matrix-start-fixture-"));
  const file = path.join(root, "message-queue.db");
  const state = { root, file, queue: new MessageQueue(file), db: new Database(file) };
  if (token !== null) state.queue.setSyncToken(token);
  t.after(() => { state.db.close(); state.queue.close(); rmSync(root, { recursive: true, force: true }); });
  return state;
}
function noBoundary(h: ReturnType<typeof fixture>) {
  assert.equal(h.queue.getSyncToken(), cursor);
  assert.deepEqual(h.db.prepare("SELECT key FROM sync_state WHERE key LIKE 'pcp_matrix_%'").all(), []);
  assert.deepEqual(h.db.prepare("SELECT name FROM sqlite_master WHERE name LIKE 'pcp_matrix_%'").all(), []);
}

test("an explicit boundary preserves the legacy cursor without inventing retained observations", t => {
  const h = fixture(t);
  // The old constructor rejects this explicit input as an ordinary legacy
  // cursor. That is the real missing operation, not a missing-export fixture.
  const handoff = h.queue.pcpHandoff(selection, plan);
  assert.equal(handoff.checkpoint(), cursor);
  assert.deepEqual(handoff.initialBoundary(), plan);
  assert.equal(h.queue.getSyncToken(), cursor);
  assert.equal(h.db.prepare("SELECT count(*) n FROM pcp_matrix_sync_batches").get().n, 0);
  assert.equal(handoff.obligations().length, 0);
  assert.equal(JSON.stringify(handoff.initialBoundary()).includes(cursor), false);
  assert.throws(() => h.queue.setSyncToken("a-new-unowned-token"), /must commit/);
  handoff.retain(sync(), cursor);
  assert.equal(handoff.checkpoint(), "new-response");
  assert.deepEqual(handoff.obligations().map(x => x.event_id), ["$new"]);
  const batch = h.db.prepare("SELECT since_token,coverage_json FROM pcp_matrix_sync_batches").get();
  assert.equal(batch.since_token, cursor);
  assert.ok(JSON.parse(batch.coverage_json).every((x: any) => x.gaps.includes("upstream_history_unverified")));
});

test("preparing the bounded start record is pure and freezes only the exact decision inputs", () => {
  assert.deepEqual(boundaries.preparePcpLegacyStart(selection, cursor, plan.decision_ref), plan);
  assert.ok(Object.isFrozen(boundaries.preparePcpLegacyStart(selection, cursor, plan.decision_ref)));
  for (const token of ["", "x".repeat(16_385)]) {
    assert.throws(() => boundaries.preparePcpLegacyStart(selection, token, plan.decision_ref), /checkpoint/);
  }
  for (const decision of ["", "x".repeat(513), "fixture\nforged"]) {
    assert.throws(() => boundaries.preparePcpLegacyStart(selection, cursor, decision), /initial boundary/);
  }
});

test("missing, stale, mixed or malformed initial decisions reject before PCP schema or binding writes", t => {
  const changes = [undefined, { ...plan, checkpoint_sha256: digest("another-cursor") },
    { ...plan, selection_sha256: "0".repeat(64) }, { ...plan, prior_coverage: "complete" },
    { ...plan, schema: "other" }, { ...plan, decision_ref: "" }, { ...plan, extra: true }];
  for (const value of changes) {
    const h = fixture(t);
    assert.throws(() => h.queue.pcpHandoff(selection, value as any));
    noBoundary(h);
  }
  const empty = fixture(t, null);
  assert.throws(() => empty.queue.pcpHandoff(selection, plan), /legacy checkpoint/);
  assert.equal(empty.queue.getSyncToken(), null);
  assert.deepEqual(empty.db.prepare("SELECT name FROM sqlite_master WHERE name LIKE 'pcp_matrix_%'").all(), []);
});

test("a failed boundary transaction leaves the cursor, selection and schema unchanged", t => {
  const h = fixture(t);
  h.db.exec("CREATE TRIGGER reject_boundary BEFORE INSERT ON sync_state " +
    "WHEN NEW.key='pcp_matrix_start_boundary' BEGIN SELECT RAISE(ABORT,'fabricated start failure'); END");
  assert.throws(() => h.queue.pcpHandoff(selection, plan), /fabricated start failure/);
  noBoundary(h);
  h.db.exec("DROP TRIGGER reject_boundary");
  assert.equal(h.queue.pcpHandoff(selection, plan).checkpoint(), cursor);
});

test("restart reconciles the exact committed boundary without rewinding or replacing responsibility", t => {
  const h = fixture(t);
  h.queue.pcpHandoff(selection, plan);
  h.queue.close(); h.queue = new MessageQueue(h.file);
  let handoff = h.queue.pcpHandoff(selection, plan);
  assert.deepEqual(handoff.initialBoundary(), plan);
  assert.equal(handoff.obligations().length, 0);
  assert.throws(() => h.queue.pcpHandoff(selection, { ...plan, decision_ref: "fixture:replacement" }), /replace.*boundary/);
  handoff.retain(sync(), cursor);
  h.queue.close(); h.queue = new MessageQueue(h.file);
  handoff = h.queue.pcpHandoff(selection, plan);
  assert.equal(handoff.checkpoint(), "new-response");
  assert.deepEqual(handoff.initialBoundary(), plan);
  assert.equal(handoff.obligations().length, 1);
  // The already committed record is also enough for a same-selection local
  // handoff reader. Omitting it never creates a new initial boundary.
  h.queue.close(); h.queue = new MessageQueue(h.file);
  assert.equal(h.queue.pcpHandoff(selection).checkpoint(), "new-response");
});

test("a retained initial boundary cannot mask cursor corruption or replace a genuine initial-sync history", t => {
  const h = fixture(t);
  const handoff = h.queue.pcpHandoff(selection, plan);
  h.db.prepare("UPDATE sync_state SET value='unowned' WHERE key='sync_token'").run();
  assert.throws(() => handoff.checkpoint(), /no matching retained batch/);
  assert.throws(() => handoff.retain(sync(), "unowned"), /no matching retained batch/);
  assert.equal(handoff.obligations().length, 0);
  const ordinary = fixture(t, null);
  const initial = ordinary.queue.pcpHandoff(selection);
  initial.retain(sync("existing-initial-sync"), null);
  assert.throws(() => ordinary.queue.pcpHandoff(selection, plan), /replace.*boundary/);
  assert.equal(initial.checkpoint(), "existing-initial-sync");
  assert.equal(initial.initialBoundary(), null);
});

test("the actual client factory rejects an unbound or mixed initial decision before acquisition", async () => {
  const { createMatrixClient } = await import("../src/matrix/client.js");
  const config = { homeserverUrl: selection.homeserverUrl, userId: selection.userId,
    deviceId: selection.deviceId, accessToken: "fabricated", enableOAuth: false,
    enableTokenExchange: false, pcpSyncStart: plan };
  await assert.rejects(createMatrixClient(config), /requires its fixed selection/);
  await assert.rejects(createMatrixClient({ ...config, pcpSync: selection,
    pcpSyncStart: { ...plan, selection_sha256: "0".repeat(64) } }), /initial boundary/);
});
