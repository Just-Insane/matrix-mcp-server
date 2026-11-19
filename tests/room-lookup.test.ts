import assert from "node:assert/strict";
import test from "node:test";
import { createClient, MatrixEvent, type MatrixClient, type Room } from "matrix-js-sdk";
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
    getJoinedRooms: async () => ({ joined_rooms: [] }),
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
  assert.equal(page.direction, "backward");
  assert.equal(page.coverage.reachedHistoryBoundary, null);
  assert.equal(page.coverage.firstVisibleEventId, null);
  assert.equal(page.events[0].eventId, "$known");
  assert.equal(page.events[0].permalink, "https://matrix.to/#/!personal%3Aexample.org/%24known");
  assert.equal(page.events[0].status, "available");
});

test("forward native read starts at the first visible event and preserves server pagination order", async () => {
  const client = createClient({ baseUrl: "https://example.invalid", userId: "@fixture:example.invalid" });
  client.store.setSyncToken("retained-sync-cursor");
  client.getJoinedRooms = async () => ({ joined_rooms: [roomId] });
  const calls: unknown[][] = [];
  // Exercise the real SDK /messages mapping without starting sync or making network requests.
  // Bridge backfill timestamps need not agree with the server's pagination order.
  client.http.authedRequest = async (...args: unknown[]) => {
    calls.push(args);
    return { start: "first", end: "newer", chunk: [
      { ...raw("$first-visible"), type: "m.room.create", content: {} },
      { ...raw("$backfilled"), origin_server_ts: Date.parse("2020-01-01T00:00:00Z") },
    ] } as any;
  };
  const page = await readRoomHistory(client, { roomId, direction: "forward", limit: 2 });
  assert.deepEqual(calls, [["GET", "/rooms/!personal%3Aexample.org/messages", { limit: "2", dir: "f" }]]);
  assert.equal(page.direction, "forward");
  assert.deepEqual(page.events.map(event => event.eventId), ["$first-visible", "$backfilled"]);
  assert.equal(page.events[0].status, "non_message");
  assert.equal(page.coverage.firstVisibleEventId, "$first-visible");
  assert.equal(page.coverage.oldestTimestamp, "2020-01-01T00:00:00.000Z");
  assert.equal(page.coverage.nextCursor, "newer");
  assert.equal(page.coverage.endOfAccessibleHistory, false);
  assert.equal(page.coverage.reachedHistoryBoundary, null);
  assert.equal(client.store.getSyncToken(), "retained-sync-cursor");
  assert.equal(client.getRoom(roomId), null);

  client.http.authedRequest = async (...args: unknown[]) => {
    calls.push(args);
    return { start: "newer", chunk: [raw("$latest")] } as any;
  };
  const next = await readRoomHistory(client, { roomId, direction: "forward", from: page.coverage.nextCursor!, limit: 1 });
  assert.deepEqual(calls[1], ["GET", "/rooms/!personal%3Aexample.org/messages", { limit: "1", dir: "f", from: "newer" }]);
  assert.equal(next.coverage.firstVisibleEventId, null);
  assert.equal(next.coverage.endOfAccessibleHistory, true);
  assert.equal(next.coverage.reachedHistoryBoundary, "latest");
  assert.equal(client.store.getSyncToken(), "retained-sync-cursor");
});

test("empty forward pages do not claim a first visible event and retain continuation or exhaustion", async () => {
  const { client } = harness({ createMessagesRequest: async () => ({ chunk: [], end: "newer" }) });
  const page = await readRoomHistory(client, { roomId, direction: "forward", limit: 1 });
  assert.equal(page.coverage.firstVisibleEventId, null);
  assert.equal(page.coverage.nextCursor, "newer");
  assert.equal(page.coverage.endOfAccessibleHistory, false);
  assert.equal(page.coverage.reachedHistoryBoundary, null);
  const exhausted = harness({ createMessagesRequest: async () => ({ chunk: [] }) });
  const empty = await readRoomHistory(exhausted.client, { roomId, direction: "forward", limit: 1 });
  assert.equal(empty.coverage.firstVisibleEventId, null);
  assert.equal(empty.coverage.endOfAccessibleHistory, true);
  assert.equal(empty.coverage.reachedHistoryBoundary, "latest");
});

test("forward continuation preserves a repeated cursor as a stall rather than a boundary", async () => {
  const { client, calls } = harness();
  const page = await readRoomHistory(client, { roomId, direction: "forward", from: "older", limit: 1 });
  assert.deepEqual(calls, [[roomId, "older", 1, "f"]]);
  assert.equal(page.coverage.paginationStalled, true);
  assert.equal(page.coverage.firstVisibleEventId, null);
  assert.equal(page.coverage.endOfAccessibleHistory, false);
  assert.equal(page.coverage.reachedHistoryBoundary, null);
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
  const exhausted = await readRoomHistory(client, { roomId });
  assert.equal(exhausted.coverage.endOfAccessibleHistory, true);
  assert.equal(exhausted.coverage.reachedHistoryBoundary, "oldest");
  const stalled = harness({ createMessagesRequest: async () => ({ chunk: [], end: "same" }) });
  const page = await readRoomHistory(stalled.client, { roomId, from: "same" });
  assert.equal(page.coverage.paginationStalled, true);
  assert.equal(page.coverage.endOfAccessibleHistory, false);
});

test("unjoined rooms fail before any event fetch or membership mutation", async () => {
  for (const membership of ["invite", "leave", "ban", undefined]) {
    const { client, calls } = harness({ getRoom: () => membership ? { getMyMembership: () => membership } : null });
    await assert.rejects(readRoomHistory(client, { roomId }), /ROOM_NOT_JOINED/);
    await assert.rejects(readRoomHistory(client, { roomId, direction: "forward" }), /ROOM_NOT_JOINED/);
    await assert.rejects(readRoomEvent(client, roomId, "$known"), /ROOM_NOT_JOINED/);
    assert.equal(calls.length, 0);
  }
});

test("native joined membership permits bounded lookup without cached room state", async () => {
  const { client, calls } = harness({ getRoom: () => null,
    getJoinedRooms: async () => ({ joined_rooms: [roomId] }) });
  assert.equal((await readRoomHistory(client, { roomId, limit: 1 })).events[0].eventId, "$known");
  assert.equal((await readRoomEvent(client, roomId, "$known")).eventId, "$known");
  assert.deepEqual(calls, [[roomId, null, 1, "b"], [roomId, "$known"]]);
});

test("page bounds reject invalid limits before a source fetch", async () => {
  const { client, calls } = harness();
  for (const limit of [0, -1, 101, 1.5, Infinity]) {
    await assert.rejects(readRoomHistory(client, { roomId, limit }), /INVALID_LIMIT/);
  }
  assert.equal(calls.length, 0);
});

test("invalid direction is rejected before a source fetch", async () => {
  const { client, calls } = harness();
  for (const direction of ["f", "older", "", null]) {
    await assert.rejects(readRoomHistory(client, { roomId, direction: direction as any }), /INVALID_DIRECTION/);
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
