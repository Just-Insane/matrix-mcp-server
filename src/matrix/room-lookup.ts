import { Direction, EventType, type MatrixClient, type MatrixEvent } from "matrix-js-sdk";

export type LookupClient = Pick<MatrixClient,
  "getRoom" | "createMessagesRequest" | "fetchRoomEvent" |
  "getEventMapper" | "decryptEventIfNeeded"
>;

export function requireJoinedRoom(client: Pick<MatrixClient, "getRoom">, roomId: string) {
  const room = client.getRoom(roomId);
  if (!room || room.getMyMembership() !== "join") {
    throw new Error("ROOM_NOT_JOINED");
  }
  return room;
}

function timestamp(event: MatrixEvent): string | null {
  const ts = event.getTs();
  return Number.isFinite(ts) ? new Date(ts).toISOString() : null;
}

/** Text and original pointers only: no attachment downloads, key export, or queue writes. */
export async function describeRoomEvent(client: LookupClient, roomId: string, event: MatrixEvent) {
  let decryptionRejected = false;
  try {
    await client.decryptEventIfNeeded(event);
  } catch {
    decryptionRejected = true;
  }
  const eventId = event.getId();
  if (!eventId) throw new Error("INVALID_EVENT_RESPONSE");
  const base = {
    roomId,
    eventId,
    permalink: `https://matrix.to/#/${encodeURIComponent(roomId)}/${encodeURIComponent(eventId)}`,
    sender: event.getSender() ?? null,
    timestamp: timestamp(event),
    type: event.getType(),
  };
  if (event.isRedacted()) return { ...base, status: "redacted" as const };
  if (decryptionRejected || event.isDecryptionFailure() || event.getType() === EventType.RoomMessageEncrypted) {
    const code = event.decryptionFailureReason;
    return {
      ...base,
      status: "encrypted_unavailable" as const,
      // SDK reason codes distinguish missing historical keys, withheld keys, and other failures.
      decryptionFailureCode: typeof code === "string" && /^[A-Z0-9_]+$/.test(code) ? code : null,
    };
  }
  if (event.getType() !== EventType.RoomMessage) {
    return { ...base, status: "non_message" as const };
  }
  // getContent() may select a cached/bundled replacement. Keep the original ID/content paired.
  const content = event.getOriginalContent();
  const relation = content["m.relates_to"];
  return {
    ...base,
    status: "available" as const,
    contentScope: "original_event" as const,
    messageType: typeof content.msgtype === "string" ? content.msgtype : null,
    body: typeof content.body === "string" ? content.body : null,
    replyToEventId: relation?.["m.in_reply_to"]?.event_id ?? null,
    threadRootEventId: relation && ["m.thread", "io.element.thread"].includes(relation.rel_type ?? "") ? relation.event_id : null,
    replacesEventId: relation?.rel_type === "m.replace" ? relation.event_id : null,
  };
}

export async function readRoomHistory(
  client: LookupClient,
  { roomId, from, limit = 50 }: { roomId: string; from?: string; limit?: number }
) {
  requireJoinedRoom(client, roomId);
  if (!Number.isInteger(limit) || limit < 1 || limit > 100) throw new Error("INVALID_LIMIT");
  const page = await client.createMessagesRequest(roomId, from ?? null, limit, Direction.Backward);
  const mapper = client.getEventMapper();
  const events = await Promise.all(page.chunk.map(raw => {
    if (raw.room_id && raw.room_id !== roomId) throw new Error("INVALID_EVENT_RESPONSE");
    return describeRoomEvent(client, roomId, mapper({ ...raw, room_id: roomId }));
  }));
  const nextCursor = page.end ?? null;
  const dates = events.map(event => event.timestamp).filter((date): date is string => date !== null).sort();
  return {
    roomId,
    direction: "backward" as const,
    events,
    coverage: {
      scope: "this_page" as const,
      returnedEvents: events.length,
      oldestTimestamp: dates[0] ?? null,
      newestTimestamp: dates.at(-1) ?? null,
      nextCursor,
      // An empty page can still have a continuation. Only absent `end` means exhaustion.
      endOfAccessibleHistory: nextCursor === null,
      paginationStalled: nextCursor !== null && nextCursor === from,
      encryptedUnavailable: events.filter(event => event.status === "encrypted_unavailable").length,
    },
  };
}

export async function readRoomEvent(client: LookupClient, roomId: string, eventId: string) {
  requireJoinedRoom(client, roomId);
  const raw = await client.fetchRoomEvent(roomId, eventId);
  if (raw.event_id !== eventId || (raw.room_id && raw.room_id !== roomId)) {
    throw new Error("INVALID_EVENT_RESPONSE");
  }
  return describeRoomEvent(client, roomId, client.getEventMapper()({ ...raw, room_id: roomId }));
}
