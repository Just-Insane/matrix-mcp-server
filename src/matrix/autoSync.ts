/**
 * AutoSync: always-on Matrix sync loop that pushes events into the MessageQueue.
 *
 * Started once at MCP server init. Creates a persistent Matrix client with
 * event listeners for messages, reactions, and invites. All events are
 * persisted to SQLite via MessageQueue, surviving MCP restarts.
 *
 * Sync token is stored in the queue DB so the client resumes exactly where
 * it left off — no duplicate messages after restart.
 */
import { RoomEvent, MatrixEvent, MatrixEventEvent, EventType, ClientEvent, MatrixClient } from "matrix-js-sdk";
import { createConfiguredMatrixClient, getAccessToken, getMatrixContext } from "../utils/server-helpers.js";
import { getMessageQueue, QueuedMessage } from "./messageQueue.js";
import { getCachedClient } from "./clientCache.js";
import { readFileSync } from "fs";
import path from "path";

const DATA_DIR = process.env.MATRIX_DATA_DIR ?? path.join(process.cwd(), ".data");
const KEEPALIVE_INTERVAL_MS = 5 * 60 * 1000;
const SYNC_CHECK_INTERVAL_MS = 3 * 60 * 1000;

let running = false;
let keepAliveHandle: ReturnType<typeof setInterval> | null = null;
let syncCheckHandle: ReturnType<typeof setInterval> | null = null;

/**
 * Build set of DM room IDs from m.direct account data + fallback heuristic.
 */
function buildDmRoomSet(client: MatrixClient): Set<string> {
  const dmRoomIds = new Set<string>();

  const mDirectContent = (client.getAccountData(EventType.Direct) as any)?.getContent() ?? {};
  for (const rooms of Object.values(mDirectContent)) {
    if (Array.isArray(rooms)) {
      for (const rid of rooms) {
        if (typeof rid === "string" && rid.startsWith("!")) dmRoomIds.add(rid);
      }
    }
  }

  // Fallback: 2-member non-public rooms
  for (const room of client.getRooms()) {
    if (!dmRoomIds.has(room.roomId)) {
      const joinRule = room.currentState.getStateEvents("m.room.join_rules", "")?.getContent()?.join_rule;
      if (joinRule !== "public" && room.getJoinedMemberCount() === 2) {
        dmRoomIds.add(room.roomId);
      }
    }
  }

  return dmRoomIds;
}

/**
 * Extract a QueuedMessage from a Matrix event, or null if should be skipped.
 */
function extractQueuedMessage(
  event: MatrixEvent,
  ownUserId: string | null,
  dmRoomIds: Set<string>,
  client: MatrixClient,
): QueuedMessage | null {
  const evtType = event.getType();
  if (evtType !== EventType.RoomMessage && evtType !== EventType.RoomMessageEncrypted) return null;
  if (event.getSender() === ownUserId) return null;

  const content = event.getClearContent?.() || event.getContent();
  const relatesTo = content?.["m.relates_to"];
  if (relatesTo?.rel_type === "m.replace") return null;
  if (event.isRedacted()) return null;

  const eid = event.getId();
  if (!eid) return null;

  const evtRoomId = event.getRoomId() || "";
  const room = client.getRoom(evtRoomId);
  const isEncrypted = evtType === EventType.RoomMessageEncrypted;

  return {
    eventId: eid,
    roomId: evtRoomId,
    roomName: room?.name || evtRoomId,
    sender: event.getSender() || "",
    body: String(content?.body || (isEncrypted ? "[encrypted]" : "")),
    timestamp: event.getTs(),
    isDM: dmRoomIds.has(evtRoomId),
    ...(relatesTo?.rel_type === "io.element.thread" || relatesTo?.rel_type === "m.thread"
      ? { threadRootEventId: relatesTo.event_id } : {}),
    ...(relatesTo?.["m.in_reply_to"]?.event_id
      ? { replyToEventId: relatesTo["m.in_reply_to"].event_id } : {}),
    ...(isEncrypted && !content?.body ? { decryptionFailed: true } : {}),
    ...((isEncrypted && !content?.body && (event as any).decryptionFailureReason)
      ? { decryptionFailureReason: (event as any).decryptionFailureReason } : {}),
  };
}

/**
 * Start the auto-sync loop. Creates a persistent Matrix client and registers
 * event listeners that push incoming events into the MessageQueue.
 *
 * Call once at MCP server startup. Idempotent — calling again is a no-op.
 */
export async function startAutoSync(): Promise<void> {
  if (running) return;
  running = true;

  const { matrixUserId, homeserverUrl } = getMatrixContext(undefined);
  const accessToken = getAccessToken(undefined, undefined);
  const queue = getMessageQueue();

  // Sync token: prefer queue DB, fall back to old sync-state.json for migration
  let syncToken = queue.getSyncToken() || undefined;
  if (!syncToken) {
    try {
      const saved = JSON.parse(readFileSync(path.join(DATA_DIR, "sync-state.json"), "utf-8"));
      if (saved.syncToken) syncToken = saved.syncToken;
    } catch { /* no old state */ }
  }

  console.error("[autoSync] Starting...");
  const client = await createConfiguredMatrixClient(homeserverUrl, matrixUserId, accessToken, syncToken);
  const ownUserId = client.getUserId();

  // Persist current sync token
  const currentToken = client.store.getSyncToken();
  if (currentToken) queue.setSyncToken(currentToken);

  let dmRoomIds = buildDmRoomSet(client);

  // --- Catch-up scan: process events in timeline from initial sync ---
  for (const room of client.getRooms()) {
    if (room.getMyMembership() !== "join") continue;
    queue.setRoomPaginationToken(room.roomId, room.oldState.paginationToken);

    const events = room.getLiveTimeline().getEvents();
    for (const event of events) {
      const evtType = event.getType();

      // Reactions on own messages
      if (evtType === "m.reaction") {
        const relatesTo = event.getContent()?.["m.relates_to"];
        if (relatesTo?.rel_type === "m.annotation" && relatesTo.key && relatesTo.event_id) {
          const targetEvent = client.getRoom(event.getRoomId() || "")?.findEventById(relatesTo.event_id);
          if (targetEvent?.getSender() === ownUserId) {
            queue.enqueueReaction({
              eventId: event.getId() || "",
              roomId: event.getRoomId() || "",
              roomName: room.name || event.getRoomId() || "",
              sender: event.getSender() || "",
              emoji: relatesTo.key,
              reactedToEventId: relatesTo.event_id,
              timestamp: event.getTs(),
            });
          }
        }
        continue;
      }

      const msg = extractQueuedMessage(event, ownUserId, dmRoomIds, client);
      if (msg) queue.enqueueMessage(msg);
    }
  }

  // Catch-up: pending invites
  for (const room of client.getRooms()) {
    if (room.getMyMembership() === "invite") {
      const member = room.currentState.getMember(ownUserId || "");
      const invitedBy = member?.events?.member?.getSender() ?? "unknown";
      queue.enqueueInvite({
        roomId: room.roomId,
        roomName: room.name || room.roomId,
        invitedBy,
        timestamp: Date.now(),
      });
    }
  }

  // --- Live event listeners ---
  client.on(RoomEvent.Timeline, (event: MatrixEvent) => {
    const evtType = event.getType();
    const evtRoomId = event.getRoomId() || "";

    // Reactions on own messages
    if (evtType === "m.reaction") {
      const relatesTo = event.getContent()?.["m.relates_to"];
      if (relatesTo?.rel_type === "m.annotation" && relatesTo.key && relatesTo.event_id) {
        const targetEvent = client.getRoom(evtRoomId)?.findEventById(relatesTo.event_id);
        if (targetEvent?.getSender() === ownUserId) {
          queue.enqueueReaction({
            eventId: event.getId() || "",
            roomId: evtRoomId,
            roomName: client.getRoom(evtRoomId)?.name || evtRoomId,
            sender: event.getSender() || "",
            emoji: relatesTo.key,
            reactedToEventId: relatesTo.event_id,
            timestamp: event.getTs(),
          });
        }
      }
      return;
    }

    const msg = extractQueuedMessage(event, ownUserId, dmRoomIds, client);
    if (!msg) return;

    const enqueued = queue.enqueueMessage(msg);

    // For encrypted messages, listen for decryption to update body
    if (enqueued && msg.decryptionFailed) {
      event.once(MatrixEventEvent.Decrypted, () => {
        const decryptedContent = event.getClearContent?.() || event.getContent();
        if (decryptedContent?.body) {
          queue.updateDecryptedBody(msg.eventId, String(decryptedContent.body));
        }
      });
    }
  });

  // Live invite detection
  const handleInviteRoom = (room: any) => {
    if (room.getMyMembership() !== "invite") return;
    const member = room.currentState?.getMember(ownUserId || "");
    const invitedBy = member?.events?.member?.getSender() ?? "unknown";
    queue.enqueueInvite({
      roomId: room.roomId,
      roomName: room.name || room.roomId,
      invitedBy,
      timestamp: Date.now(),
    });
  };

  client.on(ClientEvent.Room, handleInviteRoom);
  client.on(RoomEvent.MyMembership, (room: any, membership: string) => {
    if (membership === "invite") handleInviteRoom(room);
  });

  // --- Maintenance intervals ---
  keepAliveHandle = setInterval(() => {
    getCachedClient(matrixUserId, homeserverUrl);
    const token = client.store.getSyncToken();
    if (token) queue.setSyncToken(token);
    dmRoomIds = buildDmRoomSet(client);
    queue.cleanup();
  }, KEEPALIVE_INTERVAL_MS);

  syncCheckHandle = setInterval(() => {
    const state = client.getSyncState();
    if (state === "STOPPED" || state === null || state === "ERROR") {
      console.error(`[autoSync] Sync unhealthy (${state})`);
    }
  }, SYNC_CHECK_INTERVAL_MS);

  console.error("[autoSync] Ready — listening for events");
}

export function stopAutoSync(): void {
  if (keepAliveHandle) { clearInterval(keepAliveHandle); keepAliveHandle = null; }
  if (syncCheckHandle) { clearInterval(syncCheckHandle); syncCheckHandle = null; }
  running = false;
  console.error("[autoSync] Stopped");
}

export function isAutoSyncRunning(): boolean {
  return running;
}
