import Database from "better-sqlite3";
import path from "path";
import { mkdirSync } from "fs";
import { EventEmitter } from "events";

const DATA_DIR = process.env.MATRIX_DATA_DIR ?? path.join(process.cwd(), ".data");
mkdirSync(DATA_DIR, { recursive: true });
const DB_PATH = path.join(DATA_DIR, "message-queue.db");

export interface QueuedMessage {
  eventId: string;
  roomId: string;
  roomName: string;
  sender: string;
  body: string;
  timestamp: number;
  isDM: boolean;
  threadRootEventId?: string;
  replyToEventId?: string;
  decryptionFailed?: boolean;
  decryptionFailureReason?: string;
}

export interface QueuedReaction {
  eventId: string;
  roomId: string;
  roomName: string;
  sender: string;
  emoji: string;
  reactedToEventId: string;
  timestamp: number;
}

export interface QueuedInvite {
  roomId: string;
  roomName: string;
  invitedBy: string;
  timestamp: number;
}

export interface QueuePeek {
  count: number;
  types: { messages: number; reactions: number; invites: number };
  rooms: { roomId: string; roomName: string; count: number }[];
}

export interface QueueContents {
  messages: QueuedMessage[];
  reactions: QueuedReaction[];
  invites: QueuedInvite[];
}

class MessageQueue extends EventEmitter {
  private db: Database.Database;

  private stmts!: {
    insertMessage: Database.Statement;
    insertReaction: Database.Statement;
    insertInvite: Database.Statement;
    peekCounts: Database.Statement;
    peekRooms: Database.Statement;
    selectUnfetched: Database.Statement;
    selectUnfetchedByRoom: Database.Statement;
    getSyncToken: Database.Statement;
    setSyncToken: Database.Statement;
    getRoomPaginationToken: Database.Statement;
    setRoomPaginationToken: Database.Statement;
    clearRoomPaginationToken: Database.Statement;
    updateDecrypted: Database.Statement;
  };

  constructor() {
    super();
    this.db = new Database(DB_PATH);
    this.db.pragma("journal_mode = WAL");
    this.db.pragma("busy_timeout = 5000");
    this.initSchema();
    this.prepareStatements();
  }

  private initSchema() {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS queued_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_type TEXT NOT NULL CHECK(event_type IN ('message', 'reaction', 'invite')),
        event_id TEXT,
        room_id TEXT NOT NULL,
        room_name TEXT NOT NULL,
        sender TEXT NOT NULL DEFAULT '',
        body TEXT,
        timestamp INTEGER NOT NULL,
        is_dm INTEGER NOT NULL DEFAULT 0,
        thread_root_event_id TEXT,
        reply_to_event_id TEXT,
        emoji TEXT,
        reacted_to_event_id TEXT,
        invited_by TEXT,
        decryption_failed INTEGER DEFAULT 0,
        decryption_failure_reason TEXT,
        fetched INTEGER NOT NULL DEFAULT 0
      );

      CREATE UNIQUE INDEX IF NOT EXISTS idx_queued_event_id
        ON queued_items(event_id) WHERE event_id IS NOT NULL;

      CREATE INDEX IF NOT EXISTS idx_queued_unfetched
        ON queued_items(fetched) WHERE fetched = 0;

      CREATE TABLE IF NOT EXISTS sync_state (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
      );
    `);
  }

  private prepareStatements() {
    this.stmts = {
      insertMessage: this.db.prepare(`
        INSERT OR IGNORE INTO queued_items
        (event_type, event_id, room_id, room_name, sender, body, timestamp, is_dm,
         thread_root_event_id, reply_to_event_id, decryption_failed, decryption_failure_reason)
        VALUES ('message', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `),
      insertReaction: this.db.prepare(`
        INSERT OR IGNORE INTO queued_items
        (event_type, event_id, room_id, room_name, sender, timestamp, emoji, reacted_to_event_id)
        VALUES ('reaction', ?, ?, ?, ?, ?, ?, ?)
      `),
      insertInvite: this.db.prepare(`
        INSERT OR IGNORE INTO queued_items
        (event_type, event_id, room_id, room_name, sender, timestamp, invited_by)
        VALUES ('invite', ?, ?, ?, ?, ?, ?)
      `),
      peekCounts: this.db.prepare(`
        SELECT event_type, COUNT(*) as cnt FROM queued_items WHERE fetched = 0 GROUP BY event_type
      `),
      peekRooms: this.db.prepare(`
        SELECT room_id, room_name, COUNT(*) as cnt FROM queued_items
        WHERE fetched = 0 AND event_type = 'message' GROUP BY room_id, room_name
      `),
      selectUnfetched: this.db.prepare(`
        SELECT * FROM queued_items WHERE fetched = 0 ORDER BY timestamp ASC
      `),
      selectUnfetchedByRoom: this.db.prepare(`
        SELECT * FROM queued_items WHERE fetched = 0 AND room_id = ? ORDER BY timestamp ASC
      `),
      getSyncToken: this.db.prepare(
        "SELECT value FROM sync_state WHERE key = 'sync_token'"
      ),
      setSyncToken: this.db.prepare(
        "INSERT OR REPLACE INTO sync_state (key, value) VALUES ('sync_token', ?)"
      ),
      getRoomPaginationToken: this.db.prepare(
        "SELECT value FROM sync_state WHERE key = ?"
      ),
      setRoomPaginationToken: this.db.prepare(
        "INSERT OR REPLACE INTO sync_state (key, value) VALUES (?, ?)"
      ),
      clearRoomPaginationToken: this.db.prepare(
        "DELETE FROM sync_state WHERE key = ?"
      ),
      updateDecrypted: this.db.prepare(`
        UPDATE queued_items SET body = ?, decryption_failed = 0, decryption_failure_reason = NULL
        WHERE event_id = ? AND fetched = 0
      `),
    };
  }

  enqueueMessage(msg: QueuedMessage): boolean {
    const result = this.stmts.insertMessage.run(
      msg.eventId, msg.roomId, msg.roomName, msg.sender, msg.body, msg.timestamp,
      msg.isDM ? 1 : 0, msg.threadRootEventId ?? null, msg.replyToEventId ?? null,
      msg.decryptionFailed ? 1 : 0, msg.decryptionFailureReason ?? null
    );
    if (result.changes > 0) {
      this.emit("new-item");
      return true;
    }
    return false;
  }

  enqueueReaction(reaction: QueuedReaction): boolean {
    const result = this.stmts.insertReaction.run(
      reaction.eventId, reaction.roomId, reaction.roomName, reaction.sender,
      reaction.timestamp, reaction.emoji, reaction.reactedToEventId
    );
    if (result.changes > 0) {
      this.emit("new-item");
      return true;
    }
    return false;
  }

  enqueueInvite(invite: QueuedInvite): boolean {
    const eventId = `invite:${invite.roomId}:${invite.timestamp}`;
    const result = this.stmts.insertInvite.run(
      eventId, invite.roomId, invite.roomName, invite.invitedBy,
      invite.timestamp, invite.invitedBy
    );
    if (result.changes > 0) {
      this.emit("new-item");
      return true;
    }
    return false;
  }

  updateDecryptedBody(eventId: string, body: string): void {
    this.stmts.updateDecrypted.run(body, eventId);
  }

  peek(): QueuePeek {
    const counts = this.stmts.peekCounts.all() as { event_type: string; cnt: number }[];
    const rooms = this.stmts.peekRooms.all() as { room_id: string; room_name: string; cnt: number }[];

    const types = { messages: 0, reactions: 0, invites: 0 };
    let total = 0;
    for (const row of counts) {
      if (row.event_type === "message") types.messages = row.cnt;
      else if (row.event_type === "reaction") types.reactions = row.cnt;
      else if (row.event_type === "invite") types.invites = row.cnt;
      total += row.cnt;
    }

    return {
      count: total,
      types,
      rooms: rooms.map(r => ({ roomId: r.room_id, roomName: r.room_name, count: r.cnt })),
    };
  }

  dequeue(roomId?: string): QueueContents {
    // Use a transaction for atomicity
    return this.db.transaction(() => {
      const rows = roomId
        ? this.stmts.selectUnfetchedByRoom.all(roomId) as any[]
        : this.stmts.selectUnfetched.all() as any[];

      if (rows.length === 0) return { messages: [], reactions: [], invites: [] };

      // Mark as fetched
      const markFetched = this.db.prepare(
        `UPDATE queued_items SET fetched = 1 WHERE id IN (${rows.map(() => "?").join(",")})`
      );
      markFetched.run(...rows.map((r: any) => r.id));

      const messages: QueuedMessage[] = [];
      const reactions: QueuedReaction[] = [];
      const invites: QueuedInvite[] = [];

      for (const row of rows) {
        if (row.event_type === "message") {
          messages.push({
            eventId: row.event_id,
            roomId: row.room_id,
            roomName: row.room_name,
            sender: row.sender,
            body: row.body || "",
            timestamp: row.timestamp,
            isDM: row.is_dm === 1,
            ...(row.thread_root_event_id ? { threadRootEventId: row.thread_root_event_id } : {}),
            ...(row.reply_to_event_id ? { replyToEventId: row.reply_to_event_id } : {}),
            ...(row.decryption_failed ? { decryptionFailed: true } : {}),
            ...(row.decryption_failure_reason ? { decryptionFailureReason: row.decryption_failure_reason } : {}),
          });
        } else if (row.event_type === "reaction") {
          reactions.push({
            eventId: row.event_id,
            roomId: row.room_id,
            roomName: row.room_name,
            sender: row.sender,
            emoji: row.emoji || "",
            reactedToEventId: row.reacted_to_event_id || "",
            timestamp: row.timestamp,
          });
        } else if (row.event_type === "invite") {
          invites.push({
            roomId: row.room_id,
            roomName: row.room_name,
            invitedBy: row.invited_by || "",
            timestamp: row.timestamp,
          });
        }
      }

      return { messages, reactions, invites };
    })();
  }

  getSyncToken(): string | null {
    const row = this.stmts.getSyncToken.get() as { value: string } | undefined;
    return row?.value ?? null;
  }

  setSyncToken(token: string): void {
    this.stmts.setSyncToken.run(token);
  }

  getRoomPaginationToken(roomId: string): string | null {
    const key = `room_pagination_token:${roomId}`;
    const row = this.stmts.getRoomPaginationToken.get(key) as { value: string } | undefined;
    return row?.value ?? null;
  }

  setRoomPaginationToken(roomId: string, token: string | null): void {
    const key = `room_pagination_token:${roomId}`;
    if (!token) {
      this.stmts.clearRoomPaginationToken.run(key);
      return;
    }
    this.stmts.setRoomPaginationToken.run(key, token);
  }

  replaySince(sinceMs: number, roomId?: string): QueueContents {
    const query = roomId
      ? "SELECT * FROM queued_items WHERE timestamp >= ? AND room_id = ? ORDER BY timestamp ASC"
      : "SELECT * FROM queued_items WHERE timestamp >= ? ORDER BY timestamp ASC";

    const rows = roomId
      ? this.db.prepare(query).all(sinceMs, roomId) as any[]
      : this.db.prepare(query).all(sinceMs) as any[];

    const messages: QueuedMessage[] = [];
    const reactions: QueuedReaction[] = [];
    const invites: QueuedInvite[] = [];

    for (const row of rows) {
      if (row.event_type === "message") {
        messages.push({
          eventId: row.event_id,
          roomId: row.room_id,
          roomName: row.room_name,
          sender: row.sender,
          body: row.body || "",
          timestamp: row.timestamp,
          isDM: row.is_dm === 1,
          ...(row.thread_root_event_id ? { threadRootEventId: row.thread_root_event_id } : {}),
          ...(row.reply_to_event_id ? { replyToEventId: row.reply_to_event_id } : {}),
          ...(row.decryption_failed ? { decryptionFailed: true } : {}),
          ...(row.decryption_failure_reason ? { decryptionFailureReason: row.decryption_failure_reason } : {}),
        });
      } else if (row.event_type === "reaction") {
        reactions.push({
          eventId: row.event_id,
          roomId: row.room_id,
          roomName: row.room_name,
          sender: row.sender,
          emoji: row.emoji || "",
          reactedToEventId: row.reacted_to_event_id || "",
          timestamp: row.timestamp,
        });
      } else if (row.event_type === "invite") {
        invites.push({
          roomId: row.room_id,
          roomName: row.room_name,
          invitedBy: row.invited_by || "",
          timestamp: row.timestamp,
        });
      }
    }

    return { messages, reactions, invites };
  }

  cleanup(maxAgeMs: number = 24 * 60 * 60 * 1000): number {
    const cutoff = Date.now() - maxAgeMs;
    const result = this.db.prepare("DELETE FROM queued_items WHERE fetched = 1 AND timestamp < ?").run(cutoff);
    return result.changes;
  }

  close(): void {
    this.db.close();
  }
}

// Singleton
let instance: MessageQueue | null = null;

export function getMessageQueue(): MessageQueue {
  if (!instance) {
    instance = new MessageQueue();
  }
  return instance;
}

export function closeMessageQueue(): void {
  if (instance) {
    instance.close();
    instance = null;
  }
}
