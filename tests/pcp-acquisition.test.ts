import assert from "node:assert/strict";
import test, { type TestContext } from "node:test";
import Database from "better-sqlite3";
import fetch, { Response } from "node-fetch";
import { chmodSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { PcpSyncHandoff, MAX_SYNC_BYTES } from "../src/matrix/pcp-sync-handoff.js";
import { canonicalJson, digest, freezeSelection, publicationContext, type PcpSyncSelection } from "../src/matrix/pcp-sync-observation.js";
import { PcpReceiver, publishPcpObservation, publicationQuery } from "../src/matrix/pcp-publication.js";
import { preparePcpPublication, PcpNativeReader, runPcpAcquisitionTick } from "../src/matrix/pcp-acquisition.js";
import { loadPcpStartup } from "../src/matrix/pcp-startup.js";

const selection: PcpSyncSelection = {
  homeserverUrl: "https://matrix.example.com", userId: "@owner:example.com", deviceId: "fixture-device",
  receiverUrl: "https://pcp.example.com/v1/source/matrix-discord",
  policy: { schema: "ea-chat-source-v2", source: "matrix", account: "fixture-account", profile: "personal",
    room_scope: "all_joined", mode: "live_and_historical", principal: "service:fixture-client",
    policy_version: "existing-policy", basis: "all-authorized-accessible-history" },
};
const ROOM = "!room:example.com", OTHER = "!other:example.com", NOW = 1_800_000_000_000;
const event = (id: string, content: any = { msgtype: "m.text", body: id }, extra: any = {}) => ({
  event_id: id, sender: selection.userId, type: "m.room.message", origin_server_ts: NOW, content, ...extra,
});
const sync = (next: string, rooms: Record<string, any>, extra: any = {}) => Buffer.from(JSON.stringify({
  next_batch: next, rooms: { join: Object.fromEntries(Object.entries(rooms).map(([room, events]) => [room,
    { timeline: { events, prev_batch: "before-" + room, limited: false, ...extra } }])) },
}));
const history = (events: any[], end?: string) => Buffer.from(JSON.stringify({ chunk: events, ...(end === undefined ? {} : { end }) }));
function fixture(t: TestContext) {
  const db = new Database(":memory:");
  db.exec("CREATE TABLE sync_state(key TEXT PRIMARY KEY,value TEXT NOT NULL)");
  const handoff = new PcpSyncHandoff(db, selection);
  t.after(() => db.close());
  return { db, handoff };
}
function positive(handoff: PcpSyncHandoff, id: string) {
  const frozen = handoff.publication(id)!, query = JSON.parse(frozen.query);
  const { room } = publicationContext(JSON.parse(frozen.body), selection);
  const item = canonicalJson([selection.policy.profile, room, query.observation_id ?? query.event_id]);
  const record = "ea-record:source-v2:" + digest(JSON.stringify(["matrix", selection.policy.account, item]));
  const revision = "source:" + digest(JSON.stringify(["matrix", selection.policy.account, item, "sha256:" + query.native_sha256]));
  return { status: "captured", read_only: true, replay_authorized: false, query_digest: frozen.queryDigest,
    capture_id: "ea-capture:" + digest(JSON.stringify(["ea-intake-v1", record, revision])), receipt_ref: "fixture-receipt",
    recordReference: { record_id: record, revision_id: revision, representation_id: "sha256:" + "a".repeat(64), lifecycle_version: 1,
      record_ref: "r2://ea-raw/sources/common/v2/" + digest(canonicalJson([record, revision])) + "/representations/" + "a".repeat(64) + ".json" },
    attachments: query.attachments,
  };
}
const client = (overrides: any = {}): any => ({
  getUserId: () => selection.userId, getDeviceId: () => selection.deviceId,
  getHomeserverUrl: () => selection.homeserverUrl, getAccessToken: () => "fabricated-native-token",
  getJoinedRooms: async () => ({ joined_rooms: [ROOM, OTHER] }), getSyncState: () => "SYNCING",
  getEventMapper: () => () => { throw new Error("No decryption requested by fixture"); },
  decryptEventIfNeeded: async () => {},
  mxcUrlToHttp: (locator: string) => selection.homeserverUrl + "/_matrix/client/v1/media/download/" + locator.slice(6),
  ...overrides,
});
function acknowledge(handoff: PcpSyncHandoff, id: string) {
  assert.ok(handoff.claim(id)); assert.equal(handoff.acknowledge(id, positive(handoff, id)), true);
}

test("all-joined selection is explicit and cannot become a provider/account/room wildcard", () => {
  assert.deepEqual(freezeSelection(selection), selection);
  for (const policy of [{ ...selection.policy, room: ROOM }, { ...selection.policy, room_scope: "all_public" },
    { ...selection.policy, mode: "live" }, { ...selection.policy, source: "discord" }]) {
    assert.throws(() => freezeSelection({ ...selection, policy } as any));
  }
});

test("same sync atomically retains own messages, native state and every joined room and accessible invitation observations", t => {
  const { handoff, db } = fixture(t);
  const wire = JSON.parse(sync("s1", { [ROOM]: [event("$own"), event("$reaction", {}, { type: "m.reaction" })],
    [OTHER]: [event("$other")] }).toString());
  wire.rooms.invite = { "!unjoined:example.com": { invite_state: { events: [event("$not-joined")] } } };
  handoff.retain(Buffer.from(JSON.stringify(wire)), null);
  assert.deepEqual(handoff.obligations().map(item => item.event_id), ["$own", "$reaction", "$other", "$not-joined"]);
  assert.ok(handoff.obligations().every(item => item.state === "ready"));
  assert.equal(handoff.checkpoint(), "s1");
  assert.equal((db.prepare("SELECT count(*) n FROM pcp_matrix_history_jobs").get() as any).n, 4);
  db.exec("CREATE TRIGGER fail_room BEFORE INSERT ON pcp_matrix_rooms BEGIN SELECT RAISE(ABORT,'fixture retention failure'); END");
  assert.throws(() => handoff.retain(sync("s2", { "!new:example.com": [event("$rollback")] }), "s1"));
  assert.equal(handoff.checkpoint(), "s1"); assert.equal(handoff.obligations().length, 4);
});

test("quiet-room inventory and empty continuation pages resume durably without changing the live cursor", t => {
  const { handoff, db } = fixture(t);
  handoff.retain(sync("live-token", {}), null);
  handoff.recordJoinedRooms([ROOM, OTHER]);
  const first = handoff.historyWork(NOW)!;
  handoff.markHistoryAttempt(first.id, NOW);
  handoff.retainHistory(first, history([], "next-page"));
  assert.equal((db.prepare("SELECT complete FROM pcp_matrix_history_jobs WHERE id=?").get(first.id) as any).complete, 0);
  assert.equal(handoff.checkpoint(), "live-token");
  const resumed = new PcpSyncHandoff(db, selection);
  const state = resumed.historyWork(NOW)!;
  assert.equal(state.kind, "state"); resumed.markHistoryAttempt(state.id, NOW);
  resumed.retainHistory(state, Buffer.from("[]"));
  const other = resumed.historyWork(NOW)!;
  assert.notEqual(other.room_id, first.room_id);
  resumed.markHistoryAttempt(other.id, NOW + 1); resumed.retainHistory(other, history([]));
  const otherState = resumed.historyWork(NOW + 2)!;
  assert.equal(otherState.kind, "state"); resumed.markHistoryAttempt(otherState.id, NOW + 2);
  resumed.retainHistory(otherState, Buffer.from("[]"));
  const continuation = resumed.historyWork(NOW + 3)!;
  assert.equal(continuation.next_token, "next-page");
  resumed.retainHistory(continuation, history([event("$old", undefined, { room_id: first.room_id })]));
  assert.equal(resumed.coverage().unfinishedHistory, 0);
  assert.equal(JSON.parse(resumed.publication(resumed.obligations()[0].id)!.body).acquisition.mode, "historical");
  assert.equal(resumed.checkpoint(), "live-token");
});

test("history custody failure and repeated cursors cannot advance or falsely complete a page", t => {
  const { handoff, db } = fixture(t);
  handoff.recordJoinedRooms([ROOM]); const work = handoff.historyWork(NOW)!;
  db.exec("CREATE TRIGGER fail_original BEFORE INSERT ON pcp_matrix_observations BEGIN SELECT RAISE(ABORT,'fixture original failure'); END");
  assert.throws(() => handoff.retainHistory(work, history([event("$old")], "cursor")));
  assert.equal(handoff.historyWork(NOW)!.next_token, null);
  assert.equal((db.prepare("SELECT count(*) n FROM pcp_matrix_history_pages").get() as any).n, 0);
  db.exec("DROP TRIGGER fail_original");
  handoff.retainHistory(work, history([], "cursor"));
  const next = handoff.historyWork(NOW)!;
  handoff.retainHistory(next, history([], "cursor"), new Date(NOW).toISOString());
  handoff.retainHistory(next, history([], "cursor"), new Date(NOW + 300_000).toISOString());
  assert.equal(handoff.coverage().unfinishedHistory, 1);
  assert.equal((db.prepare("SELECT status FROM pcp_matrix_history_jobs WHERE id=?").get(work.id) as any).status, "stalled");
});

test("limited live windows add bounded gap jobs and a known native event closes only that gap", t => {
  const { handoff, db } = fixture(t);
  handoff.retain(sync("s1", { [ROOM]: [event("$prior")] }), null);
  handoff.retain(sync("s2", { [ROOM]: [event("$new")] }, { limited: true, prev_batch: "gap-start" }), "s1");
  const work = handoff.historyWork(NOW)!;
  assert.equal(work.kind, "gap"); assert.equal(work.stop_event_id, "$prior");
  handoff.retainHistory(work, history([event("$missing"), event("$prior")], "older-still"));
  assert.equal((db.prepare("SELECT complete FROM pcp_matrix_history_jobs WHERE id=?").get(work.id) as any).complete, 1);
  assert.equal(handoff.historyWork(NOW)!.kind, "full"); assert.equal(handoff.checkpoint(), "s2");
});

test("transport spelling duplicates preserve exact originals and do not duplicate publications or collide across rooms", t => {
  const { handoff } = fixture(t);
  handoff.retain(sync("s1", { [ROOM]: [event("$same", undefined, { unsigned: { age: 1 } })],
    [OTHER]: [event("$same")] }), null);
  const work = handoff.historyWork(NOW)!;
  handoff.retainHistory(work, history([event("$same", undefined, { room_id: work.room_id, unsigned: { age: 99 } })]));
  assert.equal(handoff.obligations().length, 3);
  assert.equal(handoff.obligations().filter(item => item.state === "ready").length, 2);
  for (const item of handoff.obligations()) assert.ok(handoff.original(item.id));
});

test("immutable edits and redactions stay independently publishable in any arrival order", t => {
  const { handoff } = fixture(t);
  handoff.retain(sync("s1", { [ROOM]: [
    event("$redaction", { redacts: "$edit" }, { type: "m.room.redaction" }),
    event("$edit", { body: "* replacement", "m.new_content": { body: "replacement" },
      "m.relates_to": { rel_type: "m.replace", event_id: "$original" } }),
    event("$original"),
    event("$already-redacted", {}, { unsigned: { redacted_because: { event_id: "$native-redaction" } } }),
  ] }), null);
  const rows = handoff.obligations();
  assert.ok(rows.every(row => row.state === "ready"));
  const observations = rows.map(row => JSON.parse(handoff.publication(row.id)!.body).observation);
  assert.deepEqual(observations.map(o => o.relation), [
    { kind: "redaction", target_event_id: "$edit" }, { kind: "edit", target_event_id: "$original" },
    { kind: "original", target_event_id: null }, { kind: "original", target_event_id: null },
  ]);
  assert.ok(observations[3].gaps.includes("native_content_redacted"));
  for (const row of rows) assert.ok(handoff.original(row.id));
});

test("decrypting an already retained edit enriches that event without holding its original target", async t => {
  const { handoff } = fixture(t);
  handoff.retain(sync("s1", { [ROOM]: [event("$original"),
    event("$edit", { ciphertext: "fixture" }, { type: "m.room.encrypted" })] }), null);
  const [original, edit] = handoff.obligations(); acknowledge(handoff, edit.id);
  const available = client({ getEventMapper: () => () => ({ isDecryptionFailure: () => false, getType: () => "m.room.message",
    isRedacted: () => false, getOriginalContent: () => ({ body: "* changed", "m.new_content": { body: "changed" },
      "m.relates_to": { rel_type: "m.replace", event_id: "$original" } }) }) });
  await preparePcpPublication(handoff, edit.id, available, { media: async () => Buffer.alloc(0) }, NOW);
  assert.equal(handoff.state(original.id), "ready"); assert.equal(handoff.state(edit.id), "ready");
  const prepared = JSON.parse(handoff.publication(edit.id)!.body).observation;
  assert.deepEqual(prepared.relation, { kind: "edit", target_event_id: "$original" });
  assert.equal(prepared.decrypted_event.content["m.new_content"].body, "changed");
});

test("v2 publisher binds exact room and acquisition headers while keeping compact receipt schema", async t => {
  const { handoff } = fixture(t); handoff.recordJoinedRooms([ROOM]);
  handoff.retainHistory(handoff.historyWork(NOW)!, history([event("$history", undefined, { room_id: ROOM })]));
  const id = handoff.obligations()[0].id, operations: string[] = [];
  const receiver = new PcpReceiver(selection, { clientId: "fixture-client", clientSecret: "fabricated" },
    (async (_input, init) => {
      const headers = init!.headers as Record<string, string>; operations.push(headers["x-ea-operation"]);
      assert.equal(headers["x-ea-room"], ROOM); assert.equal(headers["x-ea-acquisition-mode"], "historical");
      if (headers["x-ea-operation"] === "reconcile") {
        assert.deepEqual(Object.keys(JSON.parse(String(init!.body))).sort(), ["attachments", "event_id", "fidelity", "native_sha256", "observation_digest", "schema"]);
      }
      return new Response(JSON.stringify(positive(handoff, id)), { status: 200 });
    }) as typeof fetch);
  assert.equal(await publishPcpObservation(handoff, id, receiver), "acknowledged");
  assert.deepEqual(operations, ["capture", "reconcile"]);
});

test("live work is chosen before historical capture and an unresolved live receipt does not monopolize recovery", async t => {
  const { handoff } = fixture(t);
  handoff.recordJoinedRooms([ROOM]); handoff.retainHistory(handoff.historyWork(NOW)!, history([event("$history")]));
  handoff.retain(sync("s1", { [ROOM]: [event("$live")] }), null);
  const sent: string[] = [], reconciled: string[] = [];
  const receiver: any = { selectionDigest: handoff.selectionDigest,
    capture: async (body: string) => { const value = JSON.parse(body); sent.push(value.observation.event_id); },
    reconcile: async (query: string) => {
      const eventId = JSON.parse(query).event_id; reconciled.push(eventId);
      const item = handoff.obligations().find(item => item.event_id === eventId)!;
      return eventId === "$live" ? { status: "absent" } : positive(handoff, item.id);
    } };
  const reader: any = { accessibleInventory: async () => sync("snapshot-only", {}), media: async () => { throw new Error("No media"); }, history: async () => history([]) };
  await runPcpAcquisitionTick({ handoff, client: client(), receiver, reader, now: () => NOW });
  assert.deepEqual(sent, ["$live", "$history"]);
  await runPcpAcquisitionTick({ handoff, client: client(), receiver, reader, now: () => NOW + 60_001 });
  assert.deepEqual(sent, ["$live", "$history"]);
  assert.equal(reconciled.filter(id => id === "$live").length, 2);
});

test("existing crypto prepares plaintext while preserving cipher bytes and pending captures cannot be rewritten", async t => {
  const { handoff, db } = fixture(t);
  handoff.retain(sync("s1", { [ROOM]: [event("$encrypted", { ciphertext: "fabricated" }, { type: "m.room.encrypted", room_id: ROOM })] }), null);
  const id = handoff.obligations()[0].id, wire = handoff.original(id)!;
  const missing = client({ getEventMapper: () => () => ({ isDecryptionFailure: () => true, getType: () => "m.room.encrypted" }) });
  await preparePcpPublication(handoff, id, missing, { media: async () => Buffer.alloc(0) }, NOW);
  acknowledge(handoff, id);
  let decrypted = 0;
  const available = client({ getEventMapper: () => () => ({ isDecryptionFailure: () => false, getType: () => "m.room.message",
    isRedacted: () => false, getOriginalContent: () => ({ msgtype: "m.text", body: "Available original plaintext" }) }),
    decryptEventIfNeeded: async () => { decrypted++; } });
  await preparePcpPublication(handoff, id, available, { media: async () => Buffer.alloc(0) }, NOW + 300_001);
  const frozen = handoff.publication(id)!;
  assert.equal(decrypted, 1); assert.deepEqual(handoff.original(id), wire);
  assert.deepEqual(Buffer.from(JSON.parse(frozen.body).provider_bytes, "base64"), wire);
  assert.equal(JSON.parse(frozen.body).observation.decrypted_event.content.body, "Available original plaintext");
  assert.equal(handoff.state(id), "ready");
  assert.equal((db.prepare("SELECT count(*) n FROM pcp_matrix_publication_archive").get() as any).n, 1);
  handoff.claim(id);
  await preparePcpPublication(handoff, id, available, { media: async () => Buffer.alloc(0) }, NOW + 600_001);
  assert.deepEqual(handoff.publication(id), frozen); assert.equal(handoff.state(id), "uncertain");
});

test("full16MiB media fits one publication and a second attachment resumes after exact parent acknowledgement", async t => {
  const { handoff } = fixture(t), bytes = Buffer.alloc(16 * 1024 * 1024, 7), thumbnail = Buffer.from("thumbnail");
  handoff.retain(sync("s1", { [ROOM]: [event("$media", { msgtype: "m.file", body: "file", url: "mxc://example.com/full",
    info: { size: bytes.length, thumbnail_url: "mxc://example.com/thumb" } })] }), null);
  const id = handoff.obligations()[0].id, reads: string[] = [];
  const reader = { media: async (locator: string) => { reads.push(locator); return locator.endsWith("/full") ? bytes : thumbnail; } };
  await preparePcpPublication(handoff, id, client(), reader, NOW);
  const first = handoff.publication(id)!;
  assert.equal(JSON.parse(first.body).attachments.length, 1);
  assert.equal(JSON.parse(first.query).attachments[0].sha256, digest(bytes));
  assert.ok(Buffer.byteLength(first.body) < 24 * 1024 * 1024);
  acknowledge(handoff, id);
  await preparePcpPublication(handoff, id, client(), reader, NOW + 300_001);
  const second = handoff.publication(id)!;
  assert.equal(JSON.parse(second.body).attachments[0].locator, "mxc://example.com/thumb");
  assert.deepEqual(reads, ["mxc://example.com/full", "mxc://example.com/thumb"]);
  assert.equal(JSON.parse(second.body).observation.gaps.includes("attachment_bytes_unavailable"), false);
  acknowledge(handoff, id);
  await preparePcpPublication(handoff, id, client(), reader, NOW + 600_001);
  assert.equal(handoff.state(id), "acknowledged");
});

test("encrypted media remains original ciphertext with explicit decryption gap and failures do not block siblings", async t => {
  const { handoff } = fixture(t), cipher = Buffer.from("fabricated ciphertext");
  handoff.retain(sync("s1", { [ROOM]: [event("$cipher", { msgtype: "m.file", body: "file",
    file: { url: "mxc://example.com/cipher", hashes: { sha256: Buffer.from(digest(cipher), "hex").toString("base64") } },
    info: { thumbnail_url: "mxc://example.com/failure" } })] }), null);
  const id = handoff.obligations()[0].id;
  await preparePcpPublication(handoff, id, client(), { media: async locator => {
    if (locator.endsWith("failure")) throw new Error("Unavailable native media"); return cipher;
  } }, NOW);
  const body = JSON.parse(handoff.publication(id)!.body);
  assert.deepEqual(Buffer.from(body.attachments[0].base64, "base64"), cipher);
  assert.ok(body.observation.gaps.includes("attachment_decryption_unavailable"));
  assert.ok(body.observation.gaps.includes("attachment_download_unavailable"));
  assert.equal(handoff.state(id), "ready");
});

test("native reader only uses authenticated bounded native paths, never a second sync or redirected media origin", async t => {
  const { handoff } = fixture(t); handoff.recordJoinedRooms([ROOM]);
  const calls: URL[] = [];
  const reader = new PcpNativeReader(client(), handoff, (async (input, init) => {
    const url = new URL(String(input)); calls.push(url);
    assert.equal(init!.method, "GET"); assert.equal(init!.redirect, "error");
    assert.equal(url.origin, selection.homeserverUrl);
    assert.equal((init!.headers as any).Authorization, "Bearer fabricated-native-token");
    return new Response(history([]));
  }) as typeof fetch);
  await reader.history(handoff.historyWork(NOW)!); await reader.media("mxc://example.com/file");
  assert.equal(calls[0].pathname.includes("/messages"), true);
  assert.equal(calls[0].searchParams.get("limit"), "100");
  assert.equal(calls.some(url => url.pathname.endsWith("/sync")), false);
  await assert.rejects(reader.media("https://outside.example/file"));
  const hostile = new PcpNativeReader(client({ mxcUrlToHttp: () => "https://outside.example/_matrix/client/v1/media/download/x/y" }), handoff);
  await assert.rejects(hostile.media("mxc://example.com/file"), /homeserver/);
  const huge = new PcpNativeReader(client(), handoff, (async () => new Response(Buffer.alloc(MAX_SYNC_BYTES + 1))) as typeof fetch);
  await assert.rejects(huge.history(handoff.historyWork(NOW)!), /byte budget/);
  assert.equal(handoff.historyWork(NOW)!.next_token, null);
  const legacyPaths: string[] = [];
  const legacy = new PcpNativeReader(client(), handoff, (async input => {
    const pathname = new URL(String(input)).pathname; legacyPaths.push(pathname);
    return pathname.startsWith("/_matrix/client/") ? new Response(null, { status: 404 }) : new Response("legacy bytes");
  }) as typeof fetch);
  assert.equal((await legacy.media("mxc://example.com/file")).toString(), "legacy bytes");
  assert.deepEqual(legacyPaths, ["/_matrix/client/v1/media/download/example.com/file", "/_matrix/media/v3/download/example.com/file"]);
});

test("pinned v2 startup enables publisher only with the matching existing private service credential", t => {
  const dir = mkdtempSync(path.join(tmpdir(), "matrix-all-joined-startup-")), file = path.join(dir, "startup.json");
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  const value = { schema: "ea-matrix-pcp-startup-v2", selection,
    receiverBinding: { clientId: "fixture-client", clientSecret: "fabricated-never-log" } };
  const set = (input: any) => { const bytes = JSON.stringify(input); writeFileSync(file, bytes); chmodSync(file, 0o600); return digest(bytes); };
  const env: any = { MATRIX_RETAINED_READ: "true", MCP_CHILD: "1", MATRIX_HOMESERVER_URL: selection.homeserverUrl,
    MATRIX_USER_ID: selection.userId, MATRIX_DEVICE_ID: selection.deviceId, MATRIX_PCP_STARTUP_FILE: file,
    MATRIX_PCP_STARTUP_SHA256: set(value) };
  assert.equal(loadPcpStartup(env)!.receiverBinding!.clientId, "fixture-client");
  env.MATRIX_PCP_STARTUP_SHA256 = set({ ...value, receiverBinding: { ...value.receiverBinding, clientId: "wrong" } });
  assert.throws(() => loadPcpStartup(env), error => !String(error).includes("fabricated-never-log"));
  env.MATRIX_PCP_STARTUP_SHA256 = set(value); chmodSync(file, 0o644);
  assert.throws(() => loadPcpStartup(env), /unsafe/);
});


test("accessible inventory retains left rooms and stripped state without adopting its live cursor", t => {
  const { handoff, db } = fixture(t);
  handoff.retain(sync("live-cursor", { [ROOM]: [event("$live")] }), null);
  const stripped = { type: "m.room.name", state_key: "", sender: selection.userId, content: { name: "Observed invitation" } };
  const snapshot = Buffer.from(JSON.stringify({ next_batch: "must-not-be-adopted", rooms: {
    join: { [ROOM]: { timeline: { events: [event("$live")], prev_batch: "initial-before" } } },
    leave: { [OTHER]: { timeline: { events: [event("$left")], prev_batch: "left-before" } } },
    invite: { "!invited:example.com": { invite_state: { events: [stripped] } } },
    knock: { "!knocked:example.com": { knock_state: { events: [stripped] } } },
  }, to_device: { events: [{ type: "m.room_key", content: { session_key: "fixture-must-not-enter-custody" } }] } }));
  const before = handoff.selectionDigest;
  handoff.retainAccessibleInventory(snapshot, new Date(NOW).toISOString());
  assert.equal(handoff.checkpoint(), "live-cursor"); assert.equal(handoff.selectionDigest, before);
  assert.equal(handoff.coverage().historicalRooms, 1); assert.equal(handoff.coverage().accessibleSnapshots, 1);
  assert.equal(handoff.accessibleInventoryDue(NOW + 1), false);
  const values = handoff.obligations().filter(x => x.state === "ready").map(x => ({ row: x, body: JSON.parse(handoff.publication(x.id)!.body) }));
  for (const { row, body } of values.filter(x => x.row.event_id === null)) {
    assert.equal(body.observation.event_id, null); assert.equal(body.observation.raw.event_id, undefined);
    assert.equal(body.observation.schema, "matrix-discord-observation/v2");
    assert.match(body.observation.observation_id, /^matrix-observation:sha256:/);
    assert.deepEqual(Buffer.from(body.provider_bytes, "base64"), handoff.original(row.id));
    assert.equal(body.acquisition.mode, "historical"); acknowledge(handoff, row.id);
  }
  assert.equal(values.filter(x => x.row.event_id === null).length, 2);
  assert.ok(!JSON.stringify(db.prepare("SELECT publication_json FROM pcp_matrix_observations").all()).includes("fixture-must-not-enter-custody"));
  handoff.recordJoinedRooms([]);
  const leftJob = db.prepare("SELECT id,next_token FROM pcp_matrix_history_jobs WHERE room_id=? AND kind='full'").get(OTHER) as any;
  assert.equal(leftJob.next_token, "left-before");
  assert.ok(handoff.historyWork(NOW)); assert.equal(handoff.coverage().unfinishedHistory, 2);
  const count = handoff.obligations().length;
  handoff.retainAccessibleInventory(snapshot, new Date(NOW + 1).toISOString());
  assert.equal(handoff.obligations().length, count);
});

test("full state retains members omitted by lazy inventory and native views never replace prior captures", t => {
  const { handoff, db } = fixture(t);
  handoff.retain(sync("s1", { [ROOM]: [event("$same", { body: "original available text" })] }), null);
  const first = handoff.obligations()[0]; acknowledge(handoff, first.id);
  const stateWork = db.prepare("SELECT id,room_id,next_token,stop_event_id,kind FROM pcp_matrix_history_jobs WHERE kind='state'").get() as any;
  const member = event("$member", { membership: "join", displayname: "Member" }, { type: "m.room.member", state_key: "@member:example.com" });
  handoff.retainHistory(stateWork, Buffer.from(JSON.stringify([member])));
  handoff.retain(sync("s2", { [ROOM]: [event("$same", {}, { unsigned: { redacted_because: { event_id: "$redactor" } } })] }), "s1");
  assert.equal(handoff.state(first.id), "acknowledged");
  const changed = handoff.obligations().find(x => x.event_id === "$same" && x.id !== first.id)!;
  assert.equal(changed.state, "ready");
  const body = JSON.parse(handoff.publication(changed.id)!.body);
  assert.equal(body.observation.event_id, "$same"); assert.equal(body.observation.raw.event_id, "$same");
  assert.equal(body.observation.provenance.kind, "native_event_view");
  assert.notEqual(body.observation.observation_id, "$same"); acknowledge(handoff, changed.id);
  assert.equal(handoff.coverage().unfinishedState, 0);
  assert.equal(handoff.checkpoint(), "s2");
});

test("inventory reads use an inline include_leave snapshot with no saved cursor or filter creation", async t => {
  const { handoff } = fixture(t); handoff.retain(sync("saved-live-cursor", {}), null);
  const calls: URL[] = [];
  const reader = new PcpNativeReader(client(), handoff, (async (input, init) => {
    const url = new URL(String(input)); calls.push(url); assert.equal(init!.method, "GET");
    return new Response(sync("different-snapshot-cursor", {}));
  }) as typeof fetch);
  handoff.retainAccessibleInventory(await reader.accessibleInventory(), new Date(NOW).toISOString());
  assert.equal(calls.length, 1); assert.equal(calls[0].searchParams.has("since"), false);
  assert.equal(JSON.parse(calls[0].searchParams.get("filter")!).room.include_leave, true);
  assert.equal(handoff.checkpoint(), "saved-live-cursor");
});

test("nested edited media can publish streamed ciphertext and a separately identified decoded rendition", async t => {
 const { handoff } = fixture(t), locator = "mxc://example.com/edited-file", cipherSha = "c".repeat(64), clearSha = "d".repeat(64);
 const file = { url: locator, v: "v2", key: { kty: "oct", alg: "A256CTR", k: "fixture" }, iv: "fixture", hashes: { sha256: Buffer.from(cipherSha, "hex").toString("base64") } };
 handoff.retain(sync("s1", { [ROOM]: [event("$large-edit", { body: "* edited file", "m.relates_to": { rel_type: "m.replace", event_id: "$earlier" },
  "m.new_content": { body: "file", msgtype: "m.file", file, info: { size: 20 * 1024 * 1024 } } })] }), null);
 const id = handoff.obligations()[0].id, decoded = "matrix-decrypted:sha256:" + digest(canonicalJson([locator, cipherSha]));
 const descriptor = (location: string, sha256: string) => {
  const item = canonicalJson([selection.policy.profile, ROOM, "$large-edit"]);
  const scope = digest(canonicalJson(["ea-original-upload-v1", ["matrix", selection.policy.account, item, digest(location)]]));
  return { object_ref: "r2://ea-raw/sources/common/streamed-originals/v1/" + scope + "/" + sha256, sha256, byte_size: 20 * 1024 * 1024,
   media_type: "application/octet-stream", source_locator: location };
 };
 const receiver = new PcpReceiver(selection, { clientId: "fixture-client", clientSecret: "fabricated" });
 const reader = { media: async () => { throw new Error("Large media must not use the inline path"); }, preparedMedia: async (_context: any, info: any) => {
  assert.equal(info.locator, locator); assert.equal(info.encrypted, true);
  return { pending: false, gaps: [], parts: [{ locator, original: descriptor(locator, cipherSha) }, { locator: decoded, original: descriptor(decoded, clearSha),
   derivation: { kind: "matrix-media-decryption" as const, source_locator: locator, source_sha256: cipherSha } }] };
 } };
 await preparePcpPublication(handoff, id, client(), reader, NOW, receiver);
 const frozen = handoff.publication(id)!, body = JSON.parse(frozen.body);
 assert.equal(body.attachments.length, 2); assert.equal(body.observation.attachments.length, 2);
 assert.equal(body.attachments.some((x: any) => x.base64 !== undefined), false);
 assert.equal(body.observation.gaps.includes("attachment_bytes_unavailable"), false);
 assert.equal(body.observation.gaps.includes("attachment_decryption_unavailable"), false);
 assert.equal(JSON.parse(frozen.query).attachments.length, 2); acknowledge(handoff, id);
 const wrong = JSON.parse(frozen.body); wrong.attachments[0].original.object_ref = "r2://ea-raw/unrelated";
 assert.throws(() => publicationQuery(JSON.stringify(wrong), selection), /bound/);
});

test("media capability upgrade retries old gaps without rewriting acknowledged or uncertain payloads", async t => {
 const { handoff, db } = fixture(t);
 handoff.retain(sync("s1", { [ROOM]: [event("$old-large", { body: "file", url: "mxc://example.com/old-large", info: { size: 20 * 1024 * 1024 } })] }), null);
 const id = handoff.obligations()[0].id;
 await preparePcpPublication(handoff, id, client(), { media: async () => { throw new Error("Old bound"); } }, NOW);
 acknowledge(handoff, id); const before = handoff.publication(id);
 db.prepare("UPDATE pcp_matrix_observations SET preparation_pending=0 WHERE id=?").run(id);
 db.prepare("DELETE FROM sync_state WHERE key='pcp_matrix_media_transfer_v1'").run();
 const resumed = new PcpSyncHandoff(db, selection);
 assert.equal(resumed.state(id), "acknowledged"); assert.deepEqual(resumed.publication(id), before);
 assert.equal((db.prepare("SELECT preparation_pending FROM pcp_matrix_observations WHERE id=?").get(id) as any).preparation_pending, 1);
});
