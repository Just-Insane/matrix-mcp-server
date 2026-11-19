import { Direction, Room, type MatrixClient, type IEvent } from "matrix-js-sdk";

export type RoomMembershipClient = Pick<MatrixClient, "getRoom" | "getJoinedRooms">;

async function joinedRoomIds(client: Pick<MatrixClient, "getJoinedRooms">): Promise<string[]> {
  const response = await client.getJoinedRooms();
  if (!Array.isArray(response.joined_rooms) || response.joined_rooms.some(id => typeof id !== "string")) {
    throw new Error("INVALID_MEMBERSHIP_RESPONSE");
  }
  return response.joined_rooms;
}

/** Membership comes from the source; absent in-memory room state is not a membership denial. */
export async function requireJoinedRoom(client: RoomMembershipClient, roomId: string): Promise<Room | null> {
  const room = client.getRoom(roomId);
  if (room?.getMyMembership() === "join") return room;
  if (!(await joinedRoomIds(client)).includes(roomId)) throw new Error("ROOM_NOT_JOINED");
  return null;
}

/** One membership request; never fetch every room's state or message history. */
export async function listJoinedRoomSummaries(client: RoomMembershipClient) {
  return (await joinedRoomIds(client)).map(roomId => {
    const room = client.getRoom(roomId);
    return {
      roomId,
      name: room?.name ?? null,
      memberCount: room?.getJoinedMemberCount() ?? null,
      stateLoaded: !!room,
    };
  });
}

/**
 * Read a single requested room when a resumed /sync has no cached state for it.
 * The temporary Room is deliberately NOT stored in the SDK's live room store:
 * sync must retain ownership of its event wiring and the automatic collector.
 * No sync cursor, queue contents, client, device, or membership is replaced.
 */
export async function loadRequestedJoinedRoom(client: MatrixClient, roomId: string, timelineLimit = 0): Promise<Room> {
  if (!Number.isInteger(timelineLimit) || timelineLimit < 0 || timelineLimit > 100) throw new Error("INVALID_LIMIT");
  const cached = await requireJoinedRoom(client, roomId);
  if (cached && (timelineLimit === 0 || cached.getLiveTimeline().getEvents().length > 0)) return cached;

  const userId = client.getUserId();
  if (!userId) throw new Error("MISSING_CLIENT_IDENTITY");
  const rawState = await client.roomState(roomId);
  const mapper = client.getEventMapper();
  const mapEvent = (raw: Partial<IEvent>) => {
    if (raw.room_id && raw.room_id !== roomId) throw new Error("INVALID_EVENT_RESPONSE");
    return mapper({ ...raw, room_id: roomId });
  };
  const room = new Room(roomId, client, userId);
  room.getLiveTimeline().initialiseState(rawState.map(mapEvent));
  room.recalculate();
  // Recheck the returned state in case membership changed during these reads.
  if (room.getMyMembership() !== "join") throw new Error("ROOM_NOT_JOINED");
  if (timelineLimit > 0) {
    const page = await client.createMessagesRequest(roomId, null, timelineLimit, Direction.Backward);
    room.addEventsToTimeline(page.chunk.map(mapEvent), true, true, room.getLiveTimeline());
    room.oldState.paginationToken = page.end ?? null;
  }
  return room;
}
