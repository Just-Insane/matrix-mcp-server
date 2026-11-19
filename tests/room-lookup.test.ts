import assert from "node:assert/strict";
import test from "node:test";
import { MatrixEvent, type MatrixClient, type Room } from "matrix-js-sdk";
import { readRoomEvent, readRoomHistory, type LookupClient } from "../src/matrix/room-lookup.js";
import { backfillRoomHistoryUntil } from "../src/matrix/history-coverage.js";

const roomId = "!personal:example.org";
const raw = (eventId = "$known") => ({
  event_id: eventId, room_id: roomId, sender: "@person:example.org",
  origin_server_ts: Date.parse("2026-11-01T12:00:00Z"),
  type: "m.room.message", content: { msgtype: "m.text", body: "synthetic known phrase" },
});

function harness(overrides: Record<string, unknown> = {}) {
  const calls: unknown[][] = [];
  const client = {
    getRoom: () => ({ getMyMembership: () => "join" }),
    getEventMapper: () => (event: any) => new MatrixEvent(event),
    decryptEventIfNeeded: async () => {},
    createMessagesRequest: async (...args: unknown[]) => {
      calls.push(args); return { chunk: [raw()], start: "start", end: "older" };
    },
    fetchRoomEvent: async (...args: unknown[]) => { calls.push(args); return raw(); },
    ...overrides,
  } as unknown as LookupClient;
  return { client, calls };
}

test("page preserves the native cursor and exact original event link", async () => {
  const { client, calls } = harness();
  const page = await readRoomHistory(client, { roomId, from: "previous", limit: 3 });
  assert.deepEqual(calls, [[roomId, "previous", 3, "b"]]);
  assert.equal(page.coverage.nextCursor, "older");
  assert.equal(page.coverage.endOfAccessibleHistory, false);
  assert.equal(page.events[0].eventId, "$known");
  assert.equal(page.events[0].permalink, "https://matrix.to/#/!personal%3Aexample.org/%24known");
  assert.equal(page.events[0].status, "available");
});

test("empty visible page is incomplete when the server provides continuation", async () => {
  const { client } = harness({ createMessagesRequest: async () => ({ chunk: [], end: "older" }) });
  const page = await readRoomHistory(client, { roomId });
  assert.equal(page.events.length, 0);
  assert.equal(page.coverage.endOfAccessibleHistory, false);
  assert.equal(page.coverage.nextCursor, "older");
});

test("only absent end marks the end of accessible history; repeated cursor marks a stall", async () => {
  const { client } = harness({ createMessagesRequest: async () => ({ chunk: [] }) });
  assert.equal((await readRoomHistory(client, { roomId })).coverage.endOfAccessibleHistory, true);
  const stalled = harness({ createMessagesRequest: async () => ({ chunk: [], end: "same" }) });
  const page = await readRoomHistory(stalled.client, { roomId, from: "same" });
  assert.equal(page.coverage.paginationStalled, true);
  assert.equal(page.coverage.endOfAccessibleHistory, false);
});

test("unjoined rooms fail before any source fetch or membership mutation", async () => {
  for (const membership of ["invite", "leave", "ban", undefined]) {
    const { client, calls } = harness({ getRoom: () => membership ? { getMyMembership: () => membership } : null });
    await assert.rejects(readRoomHistory(client, { roomId }), /ROOM_NOT_JOINED/);
    await assert.rejects(readRoomEvent(client, roomId, "$known"), /ROOM_NOT_JOINED/);
    assert.equal(calls.length, 0);
  }
});

test("page bounds reject invalid limits before a source fetch", async () => {
  const { client, calls } = harness();
  for (const limit of [0, -1, 101, 1.5, Infinity]) {
    await assert.rejects(readRoomHistory(client, { roomId, limit }), /INVALID_LIMIT/);
  }
  assert.equal(calls.length, 0);
});

test("exact read calls the event endpoint and rejects a mismatched event or room", async () => {
  const { client, calls } = harness();
  const event = await readRoomEvent(client, roomId, "$known");
  assert.deepEqual(calls, [[roomId, "$known"]]);
  assert.equal(event.status, "available");
  assert.equal("body" in event && event.body, "synthetic known phrase");
  for (const response of [raw("$other"), { ...raw(), room_id: "!other:example.org" }]) {
    const bad = harness({ fetchRoomEvent: async () => response });
    await assert.rejects(readRoomEvent(bad.client, roomId, "$known"), /INVALID_EVENT_RESPONSE/);
  }
});

test("a cached edited event returns original content with its original identity", async () => {
  const event = new MatrixEvent(raw());
  event.makeReplaced(new MatrixEvent({
    ...raw("$edit"),
    content: {
      msgtype: "m.text", body: "* replacement",
      "m.new_content": { msgtype: "m.text", body: "replacement" },
      "m.relates_to": { rel_type: "m.replace", event_id: "$known" },
    },
  }));
  assert.equal(event.getContent().body, "replacement");
  const { client } = harness({ getEventMapper: () => () => event });
  const result = await readRoomEvent(client, roomId, "$known");
  assert.equal(result.eventId, "$known");
  assert.equal("body" in result && result.body, "synthetic known phrase");
});

test("missing historical keys return an unavailable event with its original pointer", async () => {
  const encrypted = { ...raw(), type: "m.room.encrypted", content: { algorithm: "m.megolm.v1.aes-sha2" } };
  const { client } = harness({
    createMessagesRequest: async () => ({ chunk: [encrypted] }),
    getEventMapper: () => (input: any) => {
      const event = new MatrixEvent(input);
      Object.defineProperty(event, "decryptionFailureReason", { value: "HISTORICAL_MESSAGE_BACKUP_UNCONFIGURED" });
      return event;
    },
    decryptEventIfNeeded: async () => { throw new Error("synthetic missing key"); },
  });
  const page = await readRoomHistory(client, { roomId });
  assert.equal(page.events[0].status, "encrypted_unavailable");
  assert.equal("body" in page.events[0], false);
  assert.equal("decryptionFailureCode" in page.events[0] && page.events[0].decryptionFailureCode, "HISTORICAL_MESSAGE_BACKUP_UNCONFIGURED");
  assert.equal(page.coverage.encryptedUnavailable, 1);
  assert.equal(page.coverage.endOfAccessibleHistory, true);
});

test("redactions remain distinguishable from an empty message", async () => {
  const { client } = harness({ fetchRoomEvent: async () => ({ ...raw(), content: {}, unsigned: { redacted_because: {} } }) });
  const event = await readRoomEvent(client, roomId, "$known");
  assert.equal(event.status, "redacted");
  assert.equal("body" in event, false);
});

test("an authorized source error is propagated rather than converted to an empty page", async () => {
  const failure = Object.assign(new Error("synthetic denied"), { errcode: "M_FORBIDDEN" });
  const { client } = harness({ createMessagesRequest: async () => { throw failure; } });
  await assert.rejects(readRoomHistory(client, { roomId }), error => error === failure);
});

test("date backfill reports its cap and retains a continuation", async () => {
  let pages = 0;
  const room = {
    oldState: { paginationToken: "page-0" as string | null },
    getLiveTimeline: () => ({ getEvents: () => [new MatrixEvent(raw())] }),
  } as unknown as Room;
  const client = { scrollback: async () => { room.oldState.paginationToken = `page-${++pages}`; return room; } };
  const coverage = await backfillRoomHistoryUntil(client as Pick<MatrixClient, "scrollback">, room, 0);
  assert.equal(pages, 20);
  assert.equal(coverage.stoppedBecause, "batch_limit");
  assert.equal(coverage.nextCursor, "page-20");
  assert.equal(coverage.endOfAccessibleHistory, null);
});

test("date backfill continues while its SDK cursor advances but does not certify exhaustion", async () => {
  const room = { oldState: { paginationToken: "start" as string | null }, getLiveTimeline: () => ({ getEvents: () => [] }) } as unknown as Room;
  let pages = 0;
  const client = { scrollback: async () => { room.oldState.paginationToken = ++pages === 1 ? "older" : null; return room; } };
  const coverage = await backfillRoomHistoryUntil(client as Pick<MatrixClient, "scrollback">, room, 0);
  assert.equal(pages, 2);
  assert.equal(coverage.stoppedBecause, "sdk_pagination_stopped");
  assert.equal(coverage.endOfAccessibleHistory, null);
  assert.equal(coverage.nextCursor, "older");
});

test("SDK empty-page cursor loss leaves provider coverage unknown and retains a resumable page", async () => {
  const room = { oldState: { paginationToken: "before-empty" as string | null }, getLiveTimeline: () => ({ getEvents: () => [] }) } as unknown as Room;
  // Installed SDK does this for an empty chunk even when the server returned `end`.
  const client = { scrollback: async () => { room.oldState.paginationToken = null; return room; } };
  const coverage = await backfillRoomHistoryUntil(client as Pick<MatrixClient, "scrollback">, room, 0);
  assert.equal(coverage.stoppedBecause, "sdk_pagination_stopped");
  assert.equal(coverage.timelineCursorAvailable, false);
  assert.equal(coverage.endOfAccessibleHistory, null);
  assert.equal(coverage.nextCursor, "before-empty");
});

test("date backfill reports no progress without claiming exhaustion", async () => {
  const room = { oldState: { paginationToken: "same" }, getLiveTimeline: () => ({ getEvents: () => [] }) } as unknown as Room;
  const client = { scrollback: async () => room };
  const coverage = await backfillRoomHistoryUntil(client as Pick<MatrixClient, "scrollback">, room, 0);
  assert.equal(coverage.stoppedBecause, "no_progress");
  assert.equal(coverage.endOfAccessibleHistory, null);
});
