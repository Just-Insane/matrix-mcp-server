import assert from "node:assert/strict";
import test, { after, type TestContext } from "node:test";
import { existsSync, mkdtempSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { EventEmitter } from "node:events";
import Database from "better-sqlite3";
import fetch, { Response } from "node-fetch";
import { createClient, RoomEvent } from "matrix-js-sdk";
import { assertNativeSelection, retainSyncFetch, MAX_SYNC_BYTES, PcpSyncMemoryStore, preparePcpLegacyStart } from "../src/matrix/pcp-sync-handoff.js";
import { canonicalJson, digest, freezeSelection, type PcpSyncSelection } from "../src/matrix/pcp-sync-observation.js";
import { PcpReceiver, publishPcpObservation, reconcilePcpObservation } from "../src/matrix/pcp-publication.js";

// This test file never inherits a real retained session, credential or data
// directory, including when npm test is run from an operator's normal shell.
for (const name of Object.keys(process.env)) if (name.startsWith("MATRIX_")) delete process.env[name];
const moduleData = mkdtempSync(path.join(tmpdir(), "matrix-pcp-module-fixture-"));
process.env.MATRIX_DATA_DIR = moduleData;
const originalFetch = globalThis.fetch;
globalThis.fetch = async () => { throw new Error("Fixture forbids network outside the supplied fake fetch"); };
const { MessageQueue, getMessageQueue, closeMessageQueue, assertRetainedPcpSelection } = await import("../src/matrix/messageQueue.js");
after(() => { closeMessageQueue(); rmSync(moduleData, { recursive: true, force: true }); globalThis.fetch = originalFetch; });

const selection: PcpSyncSelection = {
  homeserverUrl: "https://matrix.example.com", userId: "@owner:example.com", deviceId: "FIXTURE",
  receiverUrl: "https://pcp.example.com/v1/source/matrix-discord",
  policy: {
    schema: "ea-chat-source-v1", source: "matrix", account: "@owner:example.com",
    room: "!selected:example.com", profile: "personal", principal: "service:fixture",
    policy_version: "fixture-v1", mode: "live", basis: "explicit-selection",
  },
};
const event = (event_id = "$one", extra: any = {}) => ({
  event_id, sender: selection.userId, type: "m.room.message",
  origin_server_ts: 1_800_000_000_000, content: { msgtype: "m.text", body: "Fabricated message" }, ...extra,
});
const sync = (next = "s1", events: any[] = [event()], extra: any = {}) => Buffer.from(JSON.stringify({
  next_batch: next,
  rooms: { join: {
    [selection.policy.room]: { timeline: { events, limited: false, prev_batch: "before", ...extra }, state: { events: [] } },
    "!unselected:example.com": { timeline: { events: [event("$private", { content: { body: "UNSELECTED FIXTURE" } })] } },
  } },
}));
function fixture(t: TestContext, resumed = true, legacy = false) {
  const root = mkdtempSync(path.join(tmpdir(), "matrix-pcp-fixture-"));
  const file = path.join(root, "message-queue.db");
  const queue = new MessageQueue(file);
  if (legacy) queue.setSyncToken("legacy-start");
  const start = legacy ? preparePcpLegacyStart(selection, "legacy-start", "fixture:explicit-start") : undefined;
  const state = { root, file, queue, handoff: queue.pcpHandoff(selection, start), db: new Database(file) };
  if (resumed) state.handoff.retain(sync("s0", []), null);
  t.after(() => { if (state.db.open) state.db.close(); state.queue.close(); rmSync(root, { recursive: true, force: true }); });
  return state;
}
function restart(h: ReturnType<typeof fixture>) {
  h.db.close(); h.queue.close();
  h.queue = new MessageQueue(h.file); h.handoff = h.queue.pcpHandoff(selection); h.db = new Database(h.file);
}
function positive(h: ReturnType<typeof fixture>, id: string) {
  const frozen = h.handoff.publication(id)!;
  const query = JSON.parse(frozen.query), item = canonicalJson([selection.policy.profile, selection.policy.room, query.event_id]);
  const record = "ea-record:source-v2:" + digest(JSON.stringify(["matrix", selection.policy.account, item]));
  const revision = "source:" + digest(JSON.stringify(["matrix", selection.policy.account, item, "sha256:" + query.native_sha256]));
  const representation = "ea-rep:" + "a".repeat(64);
  return {
    status: "captured", read_only: true, replay_authorized: false, query_digest: frozen.queryDigest,
    capture_id: "ea-capture:" + digest(JSON.stringify(["ea-intake-v1", record, revision])),
    recordReference: { record_id: record, revision_id: revision, representation_id: representation, lifecycle_version: 1,
      record_ref: "r2://ea-raw/sources/common/v2/" + digest(canonicalJson([record, revision])) + "/representations/" + "a".repeat(64) + ".json" },
    receipt_ref: "fixture-receipt", attachments: [],
  };
}
function receiver(handler: (operation: string, body: string) => Promise<unknown>) {
  return new PcpReceiver(selection, {
    clientId: "fixture", clientSecret: "fabricated",
  }, (async (input, init) => {
    assert.equal(String(input), "https://pcp.example.com/v1/source/matrix-discord");
    assert.equal(init?.method, "POST"); assert.equal(init?.redirect, "error");
    const headers = init?.headers as Record<string, string>;
    assert.equal(headers["x-ea-account"], selection.policy.account);
    assert.equal(headers["x-ea-room"], selection.policy.room);
    assert.equal(headers["CF-Access-Client-Id"], "fixture");
    return new Response(JSON.stringify(await handler(headers["x-ea-operation"], String(init?.body))), { status: 200 });
  }) as typeof fetch);
}

test("legacy cursors cannot create PCP custody and a checkpoint requires its retained batch", t => {
  const root = mkdtempSync(path.join(tmpdir(), "matrix-pcp-legacy-fixture-"));
  const file = path.join(root, "message-queue.db"), queue = new MessageQueue(file), db = new Database(file);
  t.after(() => { db.close(); queue.close(); rmSync(root, { recursive: true, force: true }); });
  queue.setSyncToken("legacy-without-custody");
  assert.throws(() => queue.pcpHandoff(selection), /legacy Matrix cursor/);
  assert.equal(queue.getSyncToken(), "legacy-without-custody");
  assert.equal(db.prepare("SELECT value FROM sync_state WHERE key='pcp_matrix_selection'").get(), undefined);
  assert.deepEqual(db.prepare("SELECT name FROM sqlite_master WHERE name LIKE 'pcp_matrix_%'").all(), []);
  const h = fixture(t);
  h.db.prepare("UPDATE sync_state SET value='unowned' WHERE key='sync_token'").run();
  assert.throws(() => h.handoff.checkpoint(), /no matching retained batch/);
  assert.throws(() => h.handoff.retain(sync(), "unowned"), /no matching retained batch/);
  assert.equal(h.handoff.obligations().length, 0);
});

test("startup selection preflight does not create a queue and still rejects an omitted retained binding", t => {
  const root = mkdtempSync(path.join(tmpdir(), "matrix-pcp-preflight-fixture-"));
  const file = path.join(root, "message-queue.db");
  t.after(() => rmSync(root, { recursive: true, force: true }));
  assertRetainedPcpSelection(undefined, file);
  assert.equal(existsSync(file), false);
  assert.deepEqual(readdirSync(root), []);
  writeFileSync(file + "-wal", "fabricated orphaned WAL");
  assert.throws(() => assertRetainedPcpSelection(undefined, file), /custody is incomplete/);
  assert.equal(existsSync(file), false);
  const h = fixture(t);
  assertRetainedPcpSelection(selection, h.file);
  assert.throws(() => assertRetainedPcpSelection(undefined, h.file), /fixed selection/);
  assert.throws(() => assertRetainedPcpSelection({ ...selection, deviceId: "OTHER" }, h.file), /fixed selection/);
  assert.equal(h.handoff.checkpoint(), "s0");
  assert.equal(h.handoff.obligations().length, 0);
});

test("wire custody and checkpoint commit before SDK-visible response; no other-room bytes are retained", async t => {
  const h = fixture(t);
  const wire = '{ "event_id":"$escaped", "sender":"@owner:example.com","type":"m.room.message",' +
    '"origin_server_ts":1800000000000,"content":{"msgtype":"m.text","body":"caf\\u00e9 ✨"}}';
  const envelope = JSON.parse(sync().toString());
  const unrelated = { events: [{ type: "unrelated", content: { body: "UNSELECTED FIXTURE" } }] };
  envelope.account_data = unrelated; envelope.to_device = unrelated; envelope.presence = unrelated;
  envelope.device_lists = { changed: ["UNSELECTED FIXTURE DEVICE"] };
  envelope.rooms.join[selection.policy.room].account_data = unrelated;
  envelope.rooms.join[selection.policy.room].ephemeral = unrelated;
  const body = JSON.stringify(envelope).replace(JSON.stringify(event()), wire);
  const wrapped = retainSyncFetch((async () => new Response(body)) as typeof fetch, h.handoff);
  const response = await wrapped(selection.homeserverUrl + "/_matrix/client/v3/sync?since=s0");
  assert.equal(h.queue.getSyncToken(), "s1");
  const [item] = h.handoff.obligations();
  assert.equal(h.handoff.original(item.id)?.toString(), wire);
  const published = JSON.parse(h.handoff.publication(item.id)!.body);
  assert.equal(published.provider_bytes, undefined);
  assert.equal(JSON.parse(h.handoff.publication(item.id)!.query).fidelity, "transformed");
  assert.equal(published.observation.raw.room_id, selection.policy.room);
  const rows = h.db.prepare("SELECT * FROM pcp_matrix_observations").all() as any[];
  assert.equal(rows.length, 1);
  const durable = rows.map(({ wire, ...metadata }) => Buffer.from(wire).toString() + JSON.stringify(metadata)).join() +
    JSON.stringify(h.db.prepare("SELECT * FROM pcp_matrix_sync_batches").all()) +
    JSON.stringify(h.db.prepare("SELECT * FROM pcp_matrix_batch_items").all());
  assert.ok(!durable.includes("UNSELECTED"));
  assert.deepEqual(await response.json(), JSON.parse(body));
  h.queue.enqueueMessage({ eventId: "$escaped", roomId: selection.policy.room, roomName: "Fixture",
    sender: selection.userId, body: "Rendered only", timestamp: 1, isDM: true });
  h.queue.dequeue(); h.queue.cleanup();
  assert.equal(h.handoff.original(item.id)?.toString(), wire);
  assert.equal(h.handoff.state(item.id), "ready");
});

test("native room-qualified envelopes retain original fidelity and exact spelling", t => {
  const h = fixture(t);
  h.handoff.retain(sync("s1", [event("$original", { room_id: selection.policy.room })]), "s0");
  const [item] = h.handoff.obligations(), frozen = h.handoff.publication(item.id)!;
  assert.equal(JSON.parse(frozen.query).fidelity, "original");
  assert.deepEqual(Buffer.from(JSON.parse(frozen.body).provider_bytes, "base64"), h.handoff.original(item.id));
});

test("a real SQLite failure rolls back sibling observations and checkpoint before returning to SDK", async t => {
  const h = fixture(t);
  h.db.exec("CREATE TRIGGER fixture_failure BEFORE INSERT ON pcp_matrix_observations " +
    "WHEN NEW.event_id='$fail' BEGIN SELECT RAISE(ABORT,'fabricated disk failure'); END");
  const wrapped = retainSyncFetch((async () => new Response(sync("s1", [event(), event("$fail")]))) as typeof fetch, h.handoff);
  await assert.rejects(wrapped(selection.homeserverUrl + "/_matrix/client/v3/sync?since=s0"), /fabricated disk failure/);
  assert.equal(h.queue.getSyncToken(), "s0");
  assert.equal(h.handoff.obligations().length, 0);
  assert.equal((h.db.prepare("SELECT COUNT(*) AS n FROM pcp_matrix_sync_batches").get() as any).n, 1);
});

test("restart after retention, exact replay and duplicate delivery preserve one frozen responsibility", t => {
  const h = fixture(t), bytes = sync();
  h.handoff.retain(bytes, "s0", "2026-11-20T00:00:00Z");
  const item = h.handoff.obligations()[0], frozen = h.handoff.publication(item.id)!;
  restart(h);
  assert.equal(h.handoff.checkpoint(), "s1");
  h.handoff.retain(bytes, "s0", "2026-11-20T01:00:00Z");
  h.handoff.retain(sync("s2", [event(), event("$two")]), "s1");
  assert.equal(h.handoff.obligations().length, 2);
  assert.deepEqual(h.handoff.publication(item.id), frozen);
  assert.throws(() => h.handoff.retain(bytes, "s0"), /regress retained checkpoint/);
  assert.throws(() => h.queue.setSyncToken("unretained"), /must commit/);
  assert.throws(() => h.queue.assertPcpSelection(), /requires its fixed selection/);
  assert.equal(h.handoff.checkpoint(), "s2");
});

test("decryption, attachment, limited history and malformed items stay explicit while a sibling remains useful", t => {
  const h = fixture(t);
  const body = sync("s1", [
    event("$encrypted", { type: "m.room.encrypted", content: { ciphertext: "fabricated" } }),
    event("$attachment", { content: { msgtype: "m.file", body: "Fixture PDF", url: "mxc://fixture/document" } }),
    event("$healthy"), event("$wrong-room", { room_id: "!other:example.com" }),
  ], { limited: true }).toString().replace('"$wrong-room"', '"$wrong-room"');
  h.handoff.retain(Buffer.from(body), "s0");
  const rows = h.handoff.obligations();
  assert.ok(rows.find(x => x.event_id === "$encrypted")!.gaps.includes("decryption_unavailable"));
  assert.ok(rows.find(x => x.event_id === "$attachment")!.gaps.includes("attachment_bytes_unavailable"));
  assert.ok(rows.every(x => x.gaps.includes("timeline_limited")));
  assert.equal(rows.find(x => x.event_id === "$healthy")!.state, "ready");
  assert.equal(rows.find(x => x.event_id === "$wrong-room")!.state, "held");
  const frozen = h.handoff.publication(rows.find(x => x.event_id === "$attachment")!.id)!;
  assert.deepEqual(JSON.parse(frozen.query).attachments, []);
  assert.deepEqual(JSON.parse(frozen.body).observation.attachments, [{ locator: "mxc://fixture/document", custody: "reference" }]);
  h.handoff.retain(Buffer.from('{"next_batch":"s2","rooms":{"join":{"!selected:example.com":{"timeline":{"events":[1e3]}}}}}'), "s1");
  const malformed = h.handoff.obligations().find(x => x.event_id === null)!;
  assert.equal(h.handoff.original(malformed.id)?.toString(), "1e3");
  assert.equal(malformed.state, "held");
});

test("edits, redactions and conflicting bytes preserve obligations without choosing currentness from order", t => {
  const h = fixture(t);
  h.handoff.retain(sync("s1", [
    event("$redaction", { type: "m.room.redaction", redacts: "$target" }),
    event("$edit", { content: { "m.relates_to": { rel_type: "m.replace", event_id: "$target" }, body: "replacement" } }),
    event("$target"), event("$healthy"),
  ]), "s0");
  const target = h.handoff.obligations().find(x => x.event_id === "$target")!;
  assert.equal(target.state, "held");
  assert.ok(h.handoff.obligations().filter(x => x.event_id !== "$healthy").every(x => x.gaps.includes("lifecycle_contract_pending")));
  h.handoff.retain(sync("s2", [event("$healthy", { content: { body: "Different bytes, no authority" } })]), "s1");
  assert.ok(h.handoff.obligations().filter(x => x.event_id === "$healthy").every(x => x.state === "held"));
  assert.equal(h.handoff.obligations().length, 5);
});

test("selection mismatch and non-native sync authority fail before a request or checkpoint change", async t => {
  const h = fixture(t);
  for (const change of [
    { deviceId: "OTHER" }, { userId: "@other:example.com" },
    { receiverUrl: "https://other.example.com/v1/source/matrix-discord" },
    { policy: { ...selection.policy, room: "!other:example.com" } },
    { policy: { ...selection.policy, policy_version: "other" } },
  ]) assert.throws(() => h.queue.pcpHandoff({ ...selection, ...change }), /fixed selection/);
  assert.throws(() => assertNativeSelection(selection, { ...selection, deviceId: "OTHER" }), /authenticated client/);
  assert.throws(() => assertNativeSelection(selection, { ...selection, homeserverUrl: selection.homeserverUrl + "/other" }), /authenticated client/);
  let calls = 0;
  const wrapped = retainSyncFetch((async () => { calls++; return new Response(sync()); }) as typeof fetch, h.handoff);
  for (const [url, method] of [
    ["https://other.example.com/_matrix/client/v3/sync?since=s0", "GET"],
    [selection.homeserverUrl + "/_matrix/client/v3/sync?since=s0&since=s1", "GET"],
    [selection.homeserverUrl + "/_matrix/client/v3/sync?since=s0", "POST"],
  ]) await assert.rejects(wrapped(url, { method }), /selected transport/);
  assert.equal(calls, 0);
  assert.equal(h.handoff.checkpoint(), "s0");
  assert.throws(() => freezeSelection({ ...selection, policy: { ...selection.policy, bridge: {} } } as any), /Invalid fixed/);
});

test("the actual client factory refuses attaching retention or introducing a parallel client", async t => {
  const { createMatrixClient } = await import("../src/matrix/client.js");
  const { cacheClient, shutdownAllClients } = await import("../src/matrix/clientCache.js");
  let stopped = 0;
  const cached = { getSyncState: () => "SYNCING", stopClient: () => { stopped++; } };
  cacheClient(cached as any, selection.userId, selection.homeserverUrl);
  t.after(shutdownAllClients);
  const config = { homeserverUrl: selection.homeserverUrl, userId: selection.userId,
    accessToken: "fabricated", deviceId: selection.deviceId, enableOAuth: false,
    enableTokenExchange: false, pcpSync: selection };
  await assert.rejects(createMatrixClient(config), /cannot be attached/);
  await assert.rejects(createMatrixClient({ ...config, homeserverUrl: selection.homeserverUrl + "/" }), /second Matrix client/);
  assert.equal(stopped, 0);
});

for (const initial of ["ordinary", null, "s0"] as const) test("actual autosync startup and timer preserve " +
  (initial === "ordinary" ? "ordinary cursor mirroring" : "PCP " + (initial === null ? "initial" : "resumed") + " checkpoint authority"), async t => {
  t.mock.timers.enable({ apis: ["setInterval"] });
  process.env.MATRIX_USER_ID = selection.userId;
  process.env.MATRIX_HOMESERVER_URL = selection.homeserverUrl;
  process.env.MATRIX_ACCESS_TOKEN = "fabricated";
  process.env.MATRIX_DEVICE_ID = selection.deviceId;
  const { startAutoSync, stopAutoSync } = await import("../src/matrix/autoSync.js");
  const { cacheClient, shutdownAllClients } = await import("../src/matrix/clientCache.js");
  const queue = getMessageQueue();
  const handoff = initial === "ordinary" ? undefined : queue.pcpHandoff(selection);
  if (initial === "s0") handoff!.retain(sync("s0", []), null);
  let sdkToken: string | null = initial === "ordinary" ? "ordinary-start" : initial;
  const cached = Object.assign(new EventEmitter(), {
    store: { getSyncToken: () => sdkToken }, getSyncState: () => "SYNCING", getUserId: () => selection.userId,
    getAccountData: () => undefined, getRooms: () => [], stopClient: () => {},
  });
  cacheClient(cached as any, selection.userId, selection.homeserverUrl);
  t.after(() => {
    stopAutoSync(); shutdownAllClients(); closeMessageQueue();
    for (const suffix of ["", "-wal", "-shm"]) rmSync(path.join(moduleData, "message-queue.db" + suffix), { force: true });
    for (const name of ["MATRIX_USER_ID", "MATRIX_HOMESERVER_URL", "MATRIX_ACCESS_TOKEN", "MATRIX_DEVICE_ID"]) delete process.env[name];
  });
  await startAutoSync();
  if (handoff) {
    handoff.retain(sync("s1"), initial as string | null);
    t.mock.timers.tick(5 * 60 * 1000); // the SDK still has its previous token
    assert.equal(handoff.checkpoint(), "s1");
    sdkToken = "unowned-sdk-token";
    t.mock.timers.tick(5 * 60 * 1000);
    assert.equal(handoff.checkpoint(), "s1");
    assert.throws(() => queue.setSyncToken(sdkToken!), /must commit/);
    assert.equal(handoff.obligations().length, 1);
  } else {
    assert.equal(queue.getSyncToken(), "ordinary-start");
    sdkToken = "ordinary-next";
    t.mock.timers.tick(5 * 60 * 1000);
    assert.equal(queue.getSyncToken(), "ordinary-next");
  }
});

test("truncated, ambiguous and oversized syncs cannot move the retained checkpoint", async t => {
  const h = fixture(t);
  for (const body of ['{"next_batch":"s1"', '{"next_batch":"s1","next_batch":"s2"}', "{}"]) {
    assert.throws(() => h.handoff.retain(Buffer.from(body), "s0"));
  }
  assert.throws(() => h.handoff.retain(Buffer.from('{"next_batch":"s1","account_data":"UNSELECTED SENSITIVE\\x"}'), "s0"),
    error => error instanceof Error && error.message === "Invalid Matrix JSON string");
  const wrapped = retainSyncFetch((async () => new Response(Buffer.alloc(MAX_SYNC_BYTES + 1))) as typeof fetch, h.handoff);
  await assert.rejects(wrapped(selection.homeserverUrl + "/_matrix/client/v3/sync?since=s0"), /retention bound/);
  assert.equal(h.handoff.checkpoint(), "s0");
});

test("SDK state-after and knock streams retain exact blocked items; unknown selected sections cannot be skipped", t => {
  const h = fixture(t);
  const state = event("$state", { type: "m.room.member", state_key: selection.userId, content: { membership: "join" } });
  const knock = { type: "m.room.member", state_key: selection.userId, content: { membership: "knock" } };
  h.handoff.retain(Buffer.from(JSON.stringify({
    next_batch: "s1", rooms: {
      join: { [selection.policy.room]: { "org.matrix.msc4222.state_after": { events: [state] } } },
      knock: { [selection.policy.room]: { knock_state: { events: [knock] } } },
    },
  })), "s0");
  assert.equal(h.handoff.obligations().length, 2);
  assert.ok(h.handoff.obligations().every(item => item.state === "held"));
  assert.throws(() => h.handoff.retain(Buffer.from(JSON.stringify({
    next_batch: "s2", rooms: { future: { [selection.policy.room]: { events: [event()] } } },
  })), "s1"), /Unsupported selected Matrix room section/);
  assert.equal(h.handoff.checkpoint(), "s1");
});

test("lost capture response reconciles after restart without another capture; exact receipt leaves attachment gaps", async t => {
  const h = fixture(t);
  h.handoff.retain(sync("s1", [event("$file", { content: { msgtype: "m.file", url: "mxc://fixture/file" } })]), "s0");
  const id = h.handoff.obligations()[0].id, frozen = h.handoff.publication(id)!;
  let captures = 0, reads = 0;
  const transport = receiver(async operation => {
    if (operation === "capture") {
      captures++;
      assert.equal(h.handoff.state(id), "uncertain");
      throw new Error("fabricated lost response");
    }
    reads++;
    return reads === 1 ? { status: "absent", read_only: true, replay_authorized: false } : positive(h, id);
  });
  assert.equal(await publishPcpObservation(h.handoff, id, transport), "unresolved");
  restart(h);
  assert.deepEqual(h.handoff.publication(id), frozen);
  assert.equal(await publishPcpObservation(h.handoff, id, transport), "acknowledged");
  assert.equal(captures, 1); assert.equal(reads, 2);
  assert.equal(h.handoff.state(id), "acknowledged");
  assert.ok(h.handoff.obligations()[0].gaps.includes("attachment_bytes_unavailable"));
  assert.ok(h.handoff.original(id));
});

test("a crash after durable claim, before sending, remains uncertain; healthy siblings publish independently", async t => {
  const h = fixture(t);
  h.handoff.retain(sync("s1", [event("$unknown"), event("$healthy")]), "s0");
  const [unknown, healthy] = h.handoff.obligations();
  h.handoff.claim(unknown.id); restart(h);
  let captures = 0;
  const transport = receiver(async (operation, body) => {
    if (operation === "capture") { captures++; return { status: "captured" }; }
    return JSON.parse(body).event_id === "$healthy" ? positive(h, healthy.id)
      : { status: "unavailable", read_only: true, replay_authorized: false };
  });
  assert.equal(await publishPcpObservation(h.handoff, unknown.id, transport), "unresolved");
  assert.equal(captures, 0);
  assert.equal(await publishPcpObservation(h.handoff, healthy.id, transport), "acknowledged");
  assert.equal(captures, 1);
  assert.equal(h.handoff.state(unknown.id), "uncertain");
});

test("only the exact positive read receipt can discharge; corrupt frozen custody is rejected", t => {
  const h = fixture(t);
  h.handoff.retain(sync(), "s0");
  const id = h.handoff.obligations()[0].id, good = positive(h, id);
  h.handoff.claim(id);
  const wrong = [
    { ...good, status: "absent" }, { ...good, read_only: false }, { ...good, replay_authorized: true },
    { ...good, query_digest: "0".repeat(64) }, { ...good, capture_id: "wrong" },
    { ...good, attachments: [{ locator_sha256: "0".repeat(64), sha256: "0".repeat(64) }] },
    ...["record_id", "revision_id", "representation_id", "record_ref", "lifecycle_version"].map(field =>
      ({ ...good, recordReference: { ...good.recordReference, [field]: "wrong" } })),
  ];
  for (const receipt of wrong) {
    assert.equal(h.handoff.acknowledge(id, receipt), false);
    assert.equal(h.handoff.state(id), "uncertain");
  }
  h.db.prepare("UPDATE pcp_matrix_observations SET wire=? WHERE id=?").run(Buffer.from("{}"), id);
  assert.throws(() => h.handoff.acknowledge(id, good), /digest check/);
});

test("concurrent publication calls submit at most once and capture success alone never clears responsibility", async t => {
  const h = fixture(t); h.handoff.retain(sync(), "s0");
  const id = h.handoff.obligations()[0].id;
  let captures = 0;
  const transport = receiver(async operation => {
    if (operation === "capture") { captures++; return positive(h, id); }
    return { status: "changed", read_only: true, replay_authorized: false };
  });
  await Promise.all([publishPcpObservation(h.handoff, id, transport), publishPcpObservation(h.handoff, id, transport)]);
  assert.equal(captures, 1); assert.equal(h.handoff.state(id), "uncertain");
  assert.equal(await reconcilePcpObservation(h.handoff, id, receiver(async () => positive(h, id))), "acknowledged");
});

test("uncertain publication cannot move to a different receiver or policy", async t => {
  const h = fixture(t); h.handoff.retain(sync(), "s0");
  const id = h.handoff.obligations()[0].id; h.handoff.claim(id);
  let calls = 0;
  const other = new PcpReceiver({ ...selection, receiverUrl: "https://other.example.com/v1/source/matrix-discord" },
    { clientId: "fixture", clientSecret: "fabricated" },
    (async () => { calls++; return new Response("{}"); }) as typeof fetch);
  await assert.rejects(publishPcpObservation(h.handoff, id, other), /selection mismatch/);
  assert.equal(calls, 0); assert.equal(h.handoff.state(id), "uncertain");
});

for (const initial of [null, "s0", "legacy-start"]) test("locked SDK keeps atomic responsibility through " +
  (initial === null ? "initial sync" : initial === "legacy-start" ? "explicit legacy start" : "resumed sync") +
  " and callback failure", { timeout: 6000 }, async t => {
  const h = fixture(t, initial === "s0", initial === "legacy-start");
  // The locked SDK's timeoutSignal leaves request deadline timers alive after
  // successful fake fetches. Use its real code with a controlled fixture clock,
  // while SQLite, SDK event processing and setImmediate remain real.
  t.mock.timers.enable({ apis: ["setTimeout", "setInterval"] });
  h.db.exec("CREATE TRIGGER sdk_retention_failure BEFORE INSERT ON pcp_matrix_observations " +
    "BEGIN SELECT RAISE(ABORT,'fabricated retention failure'); END");
  const cursors: (string | null)[] = [];
  let failed = false, delivered = false, callbackFailed = false;
  let resolveObserved!: () => void;
  const observed = new Promise<void>(resolve => { resolveObserved = resolve; });
  let resolveContinued!: () => void;
  const continued = new Promise<void>(resolve => { resolveContinued = resolve; });
  const fake = (async (input, init) => {
    const url = new URL(String(input));
    assert.equal(url.origin, selection.homeserverUrl);
    if (url.pathname.endsWith("/sync")) {
      cursors.push(url.searchParams.get("since"));
      if (delivered) {
        assert.equal(url.searchParams.get("since"), "s1");
        resolveContinued();
        return new Promise((_resolve, reject) => {
        const signal = init?.signal;
        if (signal?.aborted) reject(new Error("fixture stopped"));
        else signal?.addEventListener("abort", () => reject(new Error("fixture stopped")), { once: true });
        });
      }
      return new Response(sync("s1", [event("$sdk")]));
    }
    if (url.pathname.endsWith("/versions")) return new Response(JSON.stringify({ versions: ["v1.11"], unstable_features: {} }));
    if (url.pathname.endsWith("/filter")) return new Response('{"filter_id":"fixture"}');
    if (url.pathname.includes("/pushrules")) return new Response('{"global":{"content":[],"override":[],"room":[],"sender":[],"underride":[]}}');
    return new Response("{}");
  }) as typeof fetch;
  const retained = retainSyncFetch(fake, h.handoff);
  const transport = (async (input, init) => {
    try {
      const response = await retained(input, init);
      if (String(input).includes("/sync?")) delivered = true;
      return response;
    } catch (error) {
      if (!failed) {
        failed = true;
        assert.match(String(error), /fabricated retention failure/);
        assert.equal(client.store.getSyncToken(), initial);
        assert.equal(h.handoff.checkpoint(), initial);
        assert.equal(h.handoff.obligations().length, 0);
        h.db.exec("DROP TRIGGER sdk_retention_failure");
        setImmediate(() => { client.retryImmediately(); t.mock.timers.tick(0); });
      }
      throw error;
    }
  }) as typeof fetch;
  const client = createClient({
    baseUrl: selection.homeserverUrl, userId: selection.userId, deviceId: selection.deviceId,
    accessToken: "fabricated", fetchFn: transport as any, store: new PcpSyncMemoryStore(() => h.handoff),
  });
  client.store.setSyncToken(initial);
  client.on(RoomEvent.Timeline, event => {
    if (event.getId() !== "$sdk") return;
    callbackFailed = true;
    assert.equal(client.store.getSyncToken(), "s1");
    assert.equal(h.handoff.checkpoint(), "s1");
    assert.ok(h.handoff.obligations().some(x => x.event_id === "$sdk"));
    resolveObserved();
    throw new Error("fabricated presentation callback failure");
  });
  t.after(() => client.stopClient());
  await client.startClient({ initialSyncLimit: 1, pollTimeout: 1, disablePresence: true });
  await observed;
  await continued;
  assert.equal(failed, true); assert.equal(callbackFailed, true);
  assert.ok(cursors.length >= 2 && cursors[0] === initial && cursors[1] === initial);
  assert.equal(cursors.at(-1), "s1");
  client.stopClient();
  await new Promise<void>(resolve => setImmediate(resolve));
  restart(h);
  assert.equal(h.handoff.checkpoint(), "s1");
  assert.equal(h.handoff.obligations().find(x => x.event_id === "$sdk")!.state, "ready");
});
