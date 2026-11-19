import assert from "node:assert/strict";
import test from "node:test";
import { createClient, MatrixEvent, Room, type IStateEventWithRoomId } from "matrix-js-sdk";
import { listJoinedRoomSummaries, loadRequestedJoinedRoom } from "../src/matrix/room-access.js";
import { backfillRoomHistoryUntil } from "../src/matrix/history-coverage.js";

const roomId = "!personal:example.invalid";
const userId = "@fixture:example.invalid";
const state = (type: string, content: Record<string, unknown>, state_key = "") => ({
  room_id: roomId, event_id: "$state-" + type, sender: userId, state_key, type, content,
  origin_server_ts: Date.parse("2026-10-01T00:00:00Z"),
}) as IStateEventWithRoomId;
const message = (id: string, date: string) => ({
  room_id: roomId, event_id: id, sender: userId, type: "m.room.message",
  content: { msgtype: "m.text", body: "synthetic retained-room message" }, origin_server_ts: Date.parse(date),
});

function fixture() {
  // Real SDK memory store/Room/scrollback, without startClient, crypto, or a network request.
  const client = createClient({ baseUrl: "https://example.invalid", userId, accessToken: "synthetic-token" });
  client.store.setSyncToken("retained-sync-cursor");
  const calls: unknown[][] = [];
  const rawState = [state("m.room.create", { creator: userId }),
    state("m.room.member", { membership: "join", displayname: "Synthetic person" }, userId),
    state("m.room.name", { name: "Synthetic personal room" }),
    state("m.room.encryption", { algorithm: "m.megolm.v1.aes-sha2" })];
  client.getJoinedRooms = async () => { calls.push(["joined_rooms"]); return { joined_rooms: [roomId] }; };
  client.roomState = async id => { calls.push(["state", id]); return rawState; };
  client.createMessagesRequest = async (...args) => {
    calls.push(["messages", ...args]);
    return { chunk: [message("$new", "2026-11-19T12:00:00Z"), message("$older", "2026-11-18T12:00:00Z")], start: "now", end: "earlier" };
  };
  return { client, calls, rawState };
}

test("joined discovery uses native membership without loading every room's state", async () => {
  const { client, calls } = fixture();
  assert.equal(client.getRooms().length, 0);
  assert.deepEqual(await listJoinedRoomSummaries(client), [{ roomId, name: null, memberCount: null, stateLoaded: false }]);
  assert.deepEqual(calls, [["joined_rooms"]]);
  assert.equal(client.store.getSyncToken(), "retained-sync-cursor");
});

test("one requested cold room has state and bounded history without replacing the live store or cursor", async () => {
  const { client, calls } = fixture();
  const room = await loadRequestedJoinedRoom(client, roomId, 20);
  assert.equal(room.name, "Synthetic personal room");
  assert.equal(room.getMyMembership(), "join");
  assert.equal(room.hasEncryptionStateEvent(), true);
  assert.deepEqual(room.getLiveTimeline().getEvents().map(event => event.getId()), ["$older", "$new"]);
  assert.equal(room.oldState.paginationToken, "earlier");
  assert.deepEqual(calls, [["joined_rooms"], ["state", roomId], ["messages", roomId, null, 20, "b"]]);
  assert.equal(client.getRoom(roomId), null);
  assert.equal(client.store.getSyncToken(), "retained-sync-cursor");
});

test("metadata-only lookup reads state but no messages, and an existing live room is reused", async () => {
  const { client, calls, rawState } = fixture();
  await loadRequestedJoinedRoom(client, roomId);
  assert.deepEqual(calls, [["joined_rooms"], ["state", roomId]]);
  const cached = new Room(roomId, client, userId);
  cached.getLiveTimeline().initialiseState(rawState.map(raw => new MatrixEvent(raw)));
  cached.recalculate();
  client.store.storeRoom(cached);
  calls.length = 0;
  assert.equal(await loadRequestedJoinedRoom(client, roomId), cached);
  assert.deepEqual(calls, []);
});

test("unjoined or changed membership and mismatched state fail before fetching history", async () => {
  const absent = fixture();
  absent.client.getJoinedRooms = async () => ({ joined_rooms: [] });
  await assert.rejects(loadRequestedJoinedRoom(absent.client, roomId, 20), /ROOM_NOT_JOINED/);
  assert.deepEqual(absent.calls, []);
  const changed = fixture();
  changed.client.roomState = async () => [state("m.room.member", { membership: "leave" }, userId)];
  await assert.rejects(loadRequestedJoinedRoom(changed.client, roomId, 20), /ROOM_NOT_JOINED/);
  assert.deepEqual(changed.calls, [["joined_rooms"]]);
  const mismatch = fixture();
  mismatch.client.roomState = async () => [{ ...state("m.room.name", { name: "other" }), room_id: "!other:example.invalid" }];
  await assert.rejects(loadRequestedJoinedRoom(mismatch.client, roomId, 20), /INVALID_EVENT_RESPONSE/);
});

test("a membership source failure is propagated rather than presented as an empty room list", async () => {
  const { client } = fixture();
  const denied = Object.assign(new Error("synthetic membership denial"), { errcode: "M_FORBIDDEN" });
  client.getJoinedRooms = async () => { throw denied; };
  await assert.rejects(listJoinedRoomSummaries(client), error => error === denied);
  await assert.rejects(loadRequestedJoinedRoom(client, roomId), error => error === denied);
});

test("existing bounded date scrollback works on the requested-room snapshot without changing sync state", async () => {
  const { client } = fixture();
  const room = await loadRequestedJoinedRoom(client, roomId, 20);
  let pages = 0;
  client.createMessagesRequest = async (id, from, limit) => {
    assert.equal(id, roomId); assert.equal(from, "earlier"); assert.equal(limit, 100);
    pages++;
    return { chunk: [message("$historical", "2026-11-01T12:00:00Z")], start: "earlier", end: "older-again" };
  };
  const coverage = await backfillRoomHistoryUntil(client, room, Date.parse("2026-11-02T00:00:00Z"));
  assert.equal(pages, 1);
  assert.equal(coverage.stoppedBecause, "window_start_reached");
  assert.equal(coverage.nextCursor, "older-again");
  assert.equal(room.getLiveTimeline().getEvents()[0].getId(), "$historical");
  assert.equal(client.getRoom(roomId), null);
  assert.equal(client.store.getSyncToken(), "retained-sync-cursor");
});
