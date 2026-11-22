import type Database from "better-sqlite3";
import type fetch from "node-fetch";
import { Response } from "node-fetch";
import type { Readable } from "node:stream";
import { MemoryStore } from "matrix-js-sdk";
import { dirname, isAbsolute, join } from "node:path";
import { exactPositiveReceipt, publicationQuery } from "./pcp-publication.js";
import {
  canonicalJson, digest, freezeSelection, selectSync, selectHistory, selectRoomState, allJoinedSelection, nativeRoomId, publicationContext, sourceObservationId,
  type PcpSyncSelection, type SelectedObservation, type AcquisitionMode,
} from "./pcp-sync-observation.js";

export const PCP_SELECTION_KEY = "pcp_matrix_selection";
export const PCP_START_BOUNDARY_KEY = "pcp_matrix_start_boundary";
export const MAX_SYNC_BYTES = 8 * 1024 * 1024;
export const MAX_SNAPSHOT_BYTES = 64 * 1024 * 1024;
export interface PcpLegacyStart {
  schema: "ea-matrix-pcp-start-v1";
  selection_sha256: string;
  checkpoint_sha256: string;
  decision_ref: string;
  prior_coverage: "unverified";
}

/** An operator decision, not proof of earlier capture or an authorization grant.
 * The opaque cursor itself remains only in the existing sync checkpoint. */
export function freezePcpLegacyStart(input: PcpLegacyStart, selection: PcpSyncSelection): PcpLegacyStart {
  const value = JSON.parse(JSON.stringify(input));
  if (!value || typeof value !== "object" || Array.isArray(value) ||
      Object.keys(value).sort().join() !== ["schema", "selection_sha256", "checkpoint_sha256",
        "decision_ref", "prior_coverage"].sort().join() || value.schema !== "ea-matrix-pcp-start-v1" ||
      value.selection_sha256 !== digest(canonicalJson(freezeSelection(selection))) ||
      !/^[a-f0-9]{64}$/.test(value.checkpoint_sha256 ?? "") || value.prior_coverage !== "unverified" ||
      typeof value.decision_ref !== "string" || !value.decision_ref || value.decision_ref.length > 512 ||
      /[\u0000-\u001f\u007f]/.test(value.decision_ref)) {
    throw new Error("Invalid explicit Matrix PCP initial boundary");
  }
  return Object.freeze(value);
}

/** Pure preparation for a separately approved, quiescent startup. This performs
 * no database read/write, provider request, selection or automatic adoption. */
export function preparePcpLegacyStart(selection: PcpSyncSelection, checkpoint: string, decisionRef: string): PcpLegacyStart {
  if (typeof checkpoint !== "string" || !checkpoint || checkpoint.length > 16_384) {
    throw new Error("Invalid legacy Matrix checkpoint");
  }
  return freezePcpLegacyStart({ schema: "ea-matrix-pcp-start-v1",
    selection_sha256: digest(canonicalJson(freezeSelection(selection))),
    checkpoint_sha256: digest(checkpoint), decision_ref: decisionRef, prior_coverage: "unverified" }, selection);
}

function readStartBoundary(db: Database.Database, selection: PcpSyncSelection): PcpLegacyStart | null {
  const row = db.prepare("SELECT value FROM sync_state WHERE key=?").get(PCP_START_BOUNDARY_KEY) as
    { value: string } | undefined;
  if (!row) return null;
  const value = freezePcpLegacyStart(JSON.parse(row.value), selection);
  if (canonicalJson(value) !== row.value) throw new Error("Invalid retained Matrix PCP initial boundary");
  return value;
}

type State = "ready" | "uncertain" | "acknowledged" | "held";
export interface PcpObligation {
  id: string;
  event_id: string | null;
  state: State;
  gaps: string[];
}
export interface FrozenPublication {
  id: string;
  body: string;
  query: string;
  queryDigest: string;
}
export interface PcpHistoryWork {
  id: string;
  room_id: string;
  next_token: string | null;
  stop_event_id: string | null;
  kind: "full" | "gap" | "state";
}

export function assertNativeSelection(
  selection: PcpSyncSelection,
  identity: { homeserverUrl: string; userId: string | null; deviceId: string | null },
): void {
  const fixed = freezeSelection(selection);
  const home = new URL(identity.homeserverUrl);
  if (home.origin !== fixed.homeserverUrl || home.pathname !== "/" || home.search || home.hash ||
      home.username || home.password ||
      identity.userId !== fixed.userId || identity.deviceId !== fixed.deviceId) {
    throw new Error("Matrix PCP selection differs from authenticated client");
  }
}

/** One optional retention facet of MessageQueue's existing connection.
 * This is neither a second sync client nor a replacement presentation queue. */
export class PcpSyncHandoff {
  readonly selection: PcpSyncSelection;
  readonly selectionDigest: string;

  constructor(private readonly db: Database.Database, input: PcpSyncSelection, start?: PcpLegacyStart) {
    this.selection = freezeSelection(input);
    this.selectionDigest = digest(canonicalJson(this.selection));
    const requested = start === undefined ? undefined : freezePcpLegacyStart(start, this.selection);
    db.pragma("synchronous = FULL");
    db.transaction(() => {
      const existing = db.prepare("SELECT value FROM sync_state WHERE key=?")
        .get(PCP_SELECTION_KEY) as { value: string } | undefined;
      if (existing && existing.value !== canonicalJson(this.selection)) {
        throw new Error("Matrix PCP selection cannot replace retained responsibility");
      }
      const retained = readStartBoundary(db, this.selection);
      if (!existing && retained || existing && requested && canonicalJson(requested) !== canonicalJson(retained)) {
        throw new Error("Cannot replace Matrix PCP initial boundary");
      }
      if (!existing) {
        const cursor = db.prepare("SELECT value FROM sync_state WHERE key='sync_token'").get() as
          { value: string } | undefined;
        if (cursor && (!requested || !cursor.value || cursor.value.length > 16_384 ||
            requested.checkpoint_sha256 !== digest(cursor.value))) {
          throw new Error("A legacy Matrix cursor cannot seed PCP retention without its exact explicit boundary");
        }
        if (requested && !cursor) throw new Error("An explicit start requires an existing legacy checkpoint");
        if (requested && db.prepare("SELECT 1 FROM sqlite_master WHERE name IN " +
            "('pcp_matrix_sync_batches','pcp_matrix_observations','pcp_matrix_batch_items') LIMIT 1").get()) {
          throw new Error("Orphaned Matrix PCP custody cannot seed an initial boundary");
        }
      }
      db.exec(`
        CREATE TABLE IF NOT EXISTS pcp_matrix_sync_batches (
          id TEXT PRIMARY KEY, since_token TEXT, next_token TEXT NOT NULL,
          selected_digest TEXT NOT NULL, coverage_json TEXT NOT NULL,
          observed_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS pcp_matrix_observations (
          id TEXT PRIMARY KEY, event_id TEXT, wire BLOB NOT NULL,
          wire_sha256 TEXT NOT NULL, publication_json TEXT, query_json TEXT,
          query_digest TEXT, target_event_id TEXT, gaps_json TEXT NOT NULL,
          state TEXT NOT NULL CHECK(state IN ('ready','uncertain','acknowledged','held')),
          attempt_count INTEGER NOT NULL DEFAULT 0, receipt_json TEXT
        );
        CREATE INDEX IF NOT EXISTS pcp_matrix_native_event ON pcp_matrix_observations(event_id);
        CREATE INDEX IF NOT EXISTS pcp_matrix_mutation_target ON pcp_matrix_observations(target_event_id);
        CREATE TABLE IF NOT EXISTS pcp_matrix_batch_items (
          batch_id TEXT NOT NULL, observation_id TEXT NOT NULL,
          section TEXT NOT NULL, stream TEXT NOT NULL, ordinal INTEGER NOT NULL,
          PRIMARY KEY(batch_id, section, stream, ordinal)
        );
      `);
      if (allJoinedSelection(this.selection)) {
        // The original fixed-room tables/identities remain readable. Only a new
        // all-joined binding uses the acquisition facet on this same connection.
        const columns = new Set((db.prepare("PRAGMA table_info(pcp_matrix_observations)").all() as { name: string }[]).map(x => x.name));
        for (const [name, type] of [["room_id", "TEXT"], ["mode", "TEXT"], ["semantic_sha256", "TEXT"],
          ["last_attempt_ms", "INTEGER NOT NULL DEFAULT 0"], ["preparation_retry_ms", "INTEGER NOT NULL DEFAULT 0"],
          ["preparation_pending", "INTEGER NOT NULL DEFAULT 1"], ["generation", "INTEGER NOT NULL DEFAULT 0"]]) {
          if (!columns.has(name)) db.exec(`ALTER TABLE pcp_matrix_observations ADD COLUMN ${name} ${type}`);
        }
        db.exec(`
          CREATE TABLE IF NOT EXISTS pcp_matrix_rooms (
            room_id TEXT PRIMARY KEY, joined INTEGER NOT NULL, last_live_event TEXT
          );
          CREATE TABLE IF NOT EXISTS pcp_matrix_history_jobs (
            id TEXT PRIMARY KEY, room_id TEXT NOT NULL, kind TEXT NOT NULL,
            next_token TEXT, stop_event_id TEXT, complete INTEGER NOT NULL DEFAULT 0,
            last_attempt_ms INTEGER NOT NULL DEFAULT 0, retry_after_ms INTEGER NOT NULL DEFAULT 0,
            pages INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'pending'
          );
          CREATE TABLE IF NOT EXISTS pcp_matrix_history_pages (
            id TEXT PRIMARY KEY, job_id TEXT NOT NULL, from_token TEXT, next_token TEXT,
            sha256 TEXT NOT NULL, observed_at TEXT NOT NULL, item_count INTEGER NOT NULL
          );
          CREATE TABLE IF NOT EXISTS pcp_matrix_publication_archive (
            id TEXT NOT NULL, generation INTEGER NOT NULL, publication_json TEXT NOT NULL,
            query_json TEXT NOT NULL, query_digest TEXT NOT NULL, receipt_json TEXT NOT NULL,
            PRIMARY KEY(id,generation)
          );
          CREATE TABLE IF NOT EXISTS pcp_matrix_accessible_snapshots (
            id TEXT PRIMARY KEY, sha256 TEXT NOT NULL, coverage_json TEXT NOT NULL,
            observed_at TEXT NOT NULL, item_count INTEGER NOT NULL
          );
        `);
        // Existing room/job identities and their continuations stay intact.
        // The new current-state obligation complements timeline history.
        for (const room of db.prepare("SELECT room_id FROM pcp_matrix_rooms").all() as { room_id: string }[]) {
          this.enqueueHistory(room.room_id, "state", null, null);
        }
        if (!db.prepare("SELECT 1 FROM sync_state WHERE key='pcp_matrix_media_transfer_v1'").get()) {
          // A capture acknowledged before large-media/decryption support still
          // owns its explicit missing media. Retry preparation without changing
          // an attempted payload, receipt, native identity or capture state.
          db.prepare(`UPDATE pcp_matrix_observations SET preparation_pending=1,preparation_retry_ms=0
            WHERE state<>'held' AND (instr(gaps_json,'attachment_size_limit')>0 OR
              instr(gaps_json,'attachment_decryption_unavailable')>0)`).run();
          db.prepare("INSERT INTO sync_state(key,value) VALUES ('pcp_matrix_media_transfer_v1','enabled')").run();
        }
      }
      db.prepare("INSERT OR IGNORE INTO sync_state(key,value) VALUES (?,?)")
        .run(PCP_SELECTION_KEY, canonicalJson(this.selection));
      if (!existing && requested) {
        db.prepare("INSERT INTO sync_state(key,value) VALUES (?,?)")
          .run(PCP_START_BOUNDARY_KEY, canonicalJson(requested));
      }
      this.checkpoint();
    }).immediate();
  }

  /** Readback of an explicit future start never claims a retained earlier batch. */
  initialBoundary(): PcpLegacyStart | null {
    this.assertBinding();
    return readStartBoundary(this.db, this.selection);
  }

  assertStartBoundary(start?: PcpLegacyStart): void {
    if (start !== undefined && canonicalJson(freezePcpLegacyStart(start, this.selection)) !== canonicalJson(this.initialBoundary())) {
      throw new Error("Cannot replace Matrix PCP initial boundary");
    }
  }

  checkpoint(): string | null {
    this.assertBinding();
    const row = this.db.prepare("SELECT value FROM sync_state WHERE key='sync_token'").get() as
      { value: string } | undefined;
    const batch = this.db.prepare("SELECT next_token FROM pcp_matrix_sync_batches ORDER BY rowid DESC LIMIT 1")
      .get() as { next_token: string } | undefined;
    const start = this.initialBoundary();
    if (!batch && start && row && start.checkpoint_sha256 === digest(row.value)) return row.value;
    if (!batch && start) throw new Error("Matrix PCP checkpoint has no matching retained batch or explicit boundary");
    if ((row?.value ?? null) !== (batch?.next_token ?? null)) {
      throw new Error("Matrix PCP checkpoint has no matching retained batch");
    }
    return row?.value ?? null;
  }

  private assertBinding(): void {
    const row = this.db.prepare("SELECT value FROM sync_state WHERE key=?")
      .get(PCP_SELECTION_KEY) as { value: string } | undefined;
    if (row?.value !== canonicalJson(this.selection)) throw new Error("Matrix PCP selection changed");
  }

  /** The selected native bytes, independent per-item obligations, coverage and
   * the one existing sync checkpoint commit together before SDK consumption. */
  retain(bytes: Buffer, since: string | null, observedAt = new Date().toISOString()): string {
    if (bytes.length > MAX_SYNC_BYTES) throw new Error("Matrix sync exceeds retention bound");
    const previousRooms = allJoinedSelection(this.selection)
      ? new Set((this.db.prepare("SELECT room_id FROM pcp_matrix_rooms").all() as { room_id: string }[]).map(x => x.room_id))
      : new Set<string>();
    const selected = selectSync(bytes, this.selection, observedAt, previousRooms);
    const batchId = digest(canonicalJson([this.selectionDigest, since, selected.nextBatch]));
    const selectedDigest = digest(canonicalJson({
      items: selected.items.map(item => [item.wireSha256, item.section, item.stream, item.ordinal]),
      coverage: allJoinedSelection(this.selection) ? selected.coverage
        : selected.coverage.map(({ roomId: _room, lastEventId: _event, ...coverage }) => coverage),
    }));
    return this.db.transaction(() => {
      this.assertBinding();
      const previous = this.db.prepare("SELECT selected_digest FROM pcp_matrix_sync_batches WHERE id=?")
        .get(batchId) as { selected_digest: string } | undefined;
      if (previous) {
        if (previous.selected_digest !== selectedDigest || this.checkpoint() !== selected.nextBatch) {
          throw new Error("Matrix sync replay differs or would regress retained checkpoint");
        }
        return batchId;
      }
      if (this.checkpoint() !== since) throw new Error("Matrix sync did not start at retained checkpoint");
      this.db.prepare(`INSERT INTO pcp_matrix_sync_batches
        (id,since_token,next_token,selected_digest,coverage_json,observed_at) VALUES (?,?,?,?,?,?)`)
        .run(batchId, since, selected.nextBatch, selectedDigest, JSON.stringify(selected.coverage), observedAt);
      for (const item of selected.items) {
        if (allJoinedSelection(this.selection)) { this.retainItem(item, batchId); continue; }
        const id = digest(canonicalJson([this.selectionDigest, item.eventId, item.wireSha256]));
        const variant = item.eventId && this.db.prepare(
          "SELECT id FROM pcp_matrix_observations WHERE event_id=? AND wire_sha256<>? LIMIT 1")
          .get(item.eventId, item.wireSha256);
        const mutation = item.eventId && this.db.prepare(
          "SELECT id FROM pcp_matrix_observations WHERE target_event_id=? LIMIT 1").get(item.eventId);
        const gaps = [...item.gaps, ...(variant ? ["conflicting_native_observation"] : []),
          ...(mutation ? ["lifecycle_contract_pending"] : [])];
        const state: State = item.publication && !variant && !mutation ? "ready" : "held";
        this.db.prepare(`INSERT OR IGNORE INTO pcp_matrix_observations
          (id,event_id,wire,wire_sha256,publication_json,query_json,query_digest,target_event_id,gaps_json,state)
          VALUES (?,?,?,?,?,?,?,?,?,?)`).run(id, item.eventId, item.wire, item.wireSha256,
          item.publication, item.query, item.queryDigest, item.targetEventId, JSON.stringify(gaps), state);
        if (variant || item.targetEventId) {
          // Preserve every observation. A conservative publication hold is not a
          // claim that arrival order, time or this cause selected a current revision.
          const target = item.targetEventId ?? item.eventId;
          const rows = this.db.prepare(
            "SELECT id,gaps_json FROM pcp_matrix_observations WHERE event_id=? AND state='ready'")
            .all(target) as { id: string; gaps_json: string }[];
          for (const row of rows) this.db.prepare(
            "UPDATE pcp_matrix_observations SET state='held',gaps_json=? WHERE id=? AND state='ready'")
            .run(JSON.stringify([...new Set([...JSON.parse(row.gaps_json),
              item.targetEventId ? "lifecycle_contract_pending" : "conflicting_native_observation"])]), row.id);
        }
        this.db.prepare(`INSERT INTO pcp_matrix_batch_items
          (batch_id,observation_id,section,stream,ordinal) VALUES (?,?,?,?,?)`)
          .run(batchId, id, item.section, item.stream, item.ordinal);
      }
      if (allJoinedSelection(this.selection)) for (const coverage of selected.coverage) {
        if (!coverage.roomId || !["join", "leave"].includes(coverage.section)) continue;
        const previous = this.db.prepare("SELECT last_live_event FROM pcp_matrix_rooms WHERE room_id=?")
          .get(coverage.roomId) as { last_live_event: string | null } | undefined;
        this.db.prepare(`INSERT INTO pcp_matrix_rooms(room_id,joined,last_live_event) VALUES (?,?,?)
          ON CONFLICT(room_id) DO UPDATE SET joined=excluded.joined,
          last_live_event=COALESCE(excluded.last_live_event,pcp_matrix_rooms.last_live_event)`)
          .run(coverage.roomId, coverage.section === "join" ? 1 : 0, coverage.lastEventId);
        if (!["join", "leave"].includes(coverage.section)) continue;
        this.enqueueHistory(coverage.roomId, "full", coverage.prevBatch, null);
        this.enqueueHistory(coverage.roomId, "state", null, null);
        if (coverage.gaps.includes("timeline_limited") && coverage.prevBatch && previous?.last_live_event) {
          this.enqueueHistory(coverage.roomId, "gap", coverage.prevBatch, previous.last_live_event);
        }
      }
      this.db.prepare("INSERT OR REPLACE INTO sync_state(key,value) VALUES ('sync_token',?)")
        .run(selected.nextBatch);
      return batchId;
    })();
  }

  private enqueueHistory(roomId: string, kind: "full" | "gap" | "state", cursor: string | null, stop: string | null): void {
    const id = digest(canonicalJson(["ea-matrix-history-v1", this.selectionDigest, roomId, kind,
      ...(kind === "gap" ? [cursor, stop] : [])]));
    this.db.prepare(`INSERT OR IGNORE INTO pcp_matrix_history_jobs(id,room_id,kind,next_token,stop_event_id)
      VALUES (?,?,?,?,?)`).run(id, roomId, kind, cursor, stop);
  }

  private retainItem(item: SelectedObservation, batchId: string): void {
    const native = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(item.wire));
    const { unsigned, ...meaning } = native && typeof native === "object" && !Array.isArray(native) ? native : {};
    // /sync and /messages may spell room_id/unsigned age differently. Preserve
    // every exact wire observation but publish one unchanged native event.
    const semantic = digest(canonicalJson({ ...meaning, room_id: item.roomId,
      ...(unsigned?.redacted_because !== undefined ? { redacted: true } : {}) }));
    const id = digest(canonicalJson([this.selectionDigest, item.roomId, item.eventId, item.wireSha256]));
    const prior = item.eventId ? this.db.prepare(`SELECT id,semantic_sha256 FROM pcp_matrix_observations
      WHERE room_id=? AND event_id=? ORDER BY CASE WHEN semantic_sha256=? THEN 0 ELSE 1 END,rowid LIMIT 1`).get(item.roomId, item.eventId, semantic) as
      { id: string; semantic_sha256: string } | undefined : this.db.prepare(`SELECT id,semantic_sha256 FROM pcp_matrix_observations
      WHERE room_id=? AND event_id IS NULL AND semantic_sha256=? ORDER BY rowid LIMIT 1`).get(item.roomId, semantic) as
      { id: string; semantic_sha256: string } | undefined;
    const variant = prior && prior.semantic_sha256 !== semantic;
    const duplicate = prior && prior.id !== id && !variant;
    const gaps = [...new Set([...item.gaps, ...(variant ? ["native_event_view"] : []),
      ...(duplicate ? ["duplicate_native_delivery"] : [])])];
    // Matrix edits and redactions have their own immutable event IDs. Retain
    // each event independently; its relation never replaces or holds a sibling.
    if (variant && item.publication) {
      const body = JSON.parse(item.publication), o = body.observation;
      o.schema = "matrix-discord-observation/v2";
      o.provenance = { kind: "native_event_view", endpoint: item.section === "history" ? "messages" : item.section === "state_snapshot" ? "state" : "sync",
        section: item.section, stream: item.stream };
      o.observation_id = sourceObservationId(item.roomId, o.provenance.kind, o.raw, item.section, item.stream);
      o.gaps = gaps;
      item.publication = JSON.stringify(body);
      item.query = JSON.stringify(publicationQuery(item.publication, this.selection));
      item.queryDigest = digest(canonicalJson(JSON.parse(item.query)));
    }
    const state: State = item.publication && !duplicate ? "ready" : "held";
    this.db.prepare(`INSERT OR IGNORE INTO pcp_matrix_observations
      (id,event_id,wire,wire_sha256,publication_json,query_json,query_digest,target_event_id,gaps_json,state,
      room_id,mode,semantic_sha256) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`)
      .run(id, item.eventId, item.wire, item.wireSha256, item.publication, item.query, item.queryDigest,
        item.targetEventId, JSON.stringify(gaps), state, item.roomId, item.mode, semantic);
    if (item.mode === "live") {
      const original = prior?.id ?? id;
      const row = this.db.prepare(`SELECT publication_json FROM pcp_matrix_observations
        WHERE id=? AND mode='historical' AND state='ready' AND attempt_count=0`).get(original) as
        { publication_json: string } | undefined;
      if (row?.publication_json) {
        const body = JSON.parse(row.publication_json); body.acquisition = { mode: "live" };
        this.db.prepare("UPDATE pcp_matrix_observations SET mode='live',publication_json=? WHERE id=?")
          .run(JSON.stringify(body), original);
      }
    }
    this.db.prepare(`INSERT INTO pcp_matrix_batch_items(batch_id,observation_id,section,stream,ordinal)
      VALUES (?,?,?,?,?)`).run(batchId, id, item.section + ":" + item.roomId, item.stream, item.ordinal);
  }

  /** Retained items and independent history cursors survive departures.
   * Readable historical rooms continue without a new membership action. */
  recordJoinedRooms(rooms: string[]): void {
    if (!allJoinedSelection(this.selection) || !Array.isArray(rooms) || rooms.some(room => !nativeRoomId(room)) ||
        new Set(rooms).size !== rooms.length) throw new Error("Invalid complete Matrix room inventory");
    this.db.transaction(() => {
      this.assertBinding();
      this.db.prepare("UPDATE pcp_matrix_rooms SET joined=0").run();
      for (const room of rooms) {
        this.db.prepare(`INSERT INTO pcp_matrix_rooms(room_id,joined) VALUES (?,1)
          ON CONFLICT(room_id) DO UPDATE SET joined=1`).run(room);
        this.enqueueHistory(room, "full", null, null);
        this.enqueueHistory(room, "state", null, null);
      }
    }).immediate();
  }

  accessibleInventoryDue(now = Date.now()): boolean {
    const row = this.db.prepare("SELECT value FROM sync_state WHERE key='pcp_matrix_accessible_inventory'").get() as { value: string } | undefined;
    return !row || JSON.parse(row.value).retry_after_ms <= now;
  }

  markAccessibleInventory(now = Date.now(), status: "reading" | "unavailable" = "reading"): void {
    this.db.prepare("INSERT OR REPLACE INTO sync_state(key,value) VALUES ('pcp_matrix_accessible_inventory',?)")
      .run(JSON.stringify({ last_attempt_ms: now, retry_after_ms: now + 300_000, status }));
  }

  /** A read-only initial snapshot discovers left/invited/knocked rooms and
   * retains accessible state. Its next_batch is never adopted as the live
   * checkpoint and its to-device/security-control fields are never selected. */
  retainAccessibleInventory(bytes: Buffer, observedAt = new Date().toISOString()): void {
    if (!allJoinedSelection(this.selection) || bytes.length > MAX_SNAPSHOT_BYTES) throw new Error("Matrix inventory exceeds retention bound");
    const snapshot = selectSync(bytes, this.selection, observedAt, new Set(), true);
    const hash = digest(bytes), id = digest(canonicalJson(["ea-matrix-accessible-snapshot-v1", this.selectionDigest, hash]));
    this.db.transaction(() => {
      this.assertBinding();
      if (!this.db.prepare("SELECT 1 FROM pcp_matrix_accessible_snapshots WHERE id=?").get(id)) {
        this.db.prepare("INSERT INTO pcp_matrix_accessible_snapshots(id,sha256,coverage_json,observed_at,item_count) VALUES (?,?,?,?,?)")
          .run(id, hash, JSON.stringify(snapshot.coverage), observedAt, snapshot.items.length);
        for (const item of snapshot.items) this.retainItem(item, id);
        for (const coverage of snapshot.coverage) {
          if (!coverage.roomId || !["join", "leave"].includes(coverage.section)) continue;
          // The concurrent live /sync owns membership freshness. Discovery may
          // add an unknown room, but may not overwrite a newer live departure.
          this.db.prepare("INSERT OR IGNORE INTO pcp_matrix_rooms(room_id,joined) VALUES (?,?)")
            .run(coverage.roomId, coverage.section === "join" ? 1 : 0);
          this.enqueueHistory(coverage.roomId, "full", coverage.prevBatch, null);
          this.enqueueHistory(coverage.roomId, "state", null, null);
        }
      }
      this.db.prepare("INSERT OR REPLACE INTO sync_state(key,value) VALUES ('pcp_matrix_accessible_inventory',?)")
        .run(JSON.stringify({ last_attempt_ms: Date.parse(observedAt), retry_after_ms: Date.parse(observedAt) + 86_400_000,
          status: "retained", snapshot_id: id }));
    }).immediate();
  }

  historyWork(now = Date.now()): PcpHistoryWork | null {
    this.assertBinding();
    return this.db.prepare(`SELECT j.id,j.room_id,j.next_token,j.stop_event_id,j.kind
      FROM pcp_matrix_history_jobs j JOIN pcp_matrix_rooms r ON r.room_id=j.room_id
      WHERE j.complete=0 AND j.retry_after_ms<=?
      ORDER BY CASE j.kind WHEN 'gap' THEN 0 ELSE 1 END,j.last_attempt_ms,j.rowid LIMIT 1`)
      .get(now) as PcpHistoryWork | undefined ?? null;
  }

  markHistoryAttempt(id: string, now = Date.now(), status = "reading"): void {
    if (!["reading", "unavailable", "stalled"].includes(status)) throw new Error("Invalid history attempt status");
    this.db.prepare(`UPDATE pcp_matrix_history_jobs SET last_attempt_ms=?,status=?,retry_after_ms=? WHERE id=? AND complete=0`)
      .run(now, status, status === "reading" ? 0 : now + 300_000, id);
  }

  retainHistory(work: PcpHistoryWork, bytes: Buffer, observedAt = new Date().toISOString()): void {
    if (bytes.length > (work.kind === "state" ? MAX_SNAPSHOT_BYTES : MAX_SYNC_BYTES)) throw new Error("Matrix history page exceeds retention bound");
    const page = work.kind === "state" ? selectRoomState(bytes, this.selection, work.room_id, observedAt)
      : selectHistory(bytes, this.selection, work.room_id, observedAt);
    const hash = digest(bytes), id = digest(canonicalJson(["ea-matrix-history-page-v1", work.id, work.next_token, hash]));
    this.db.transaction(() => {
      this.assertBinding();
      const current = this.db.prepare("SELECT next_token,complete FROM pcp_matrix_history_jobs WHERE id=?")
        .get(work.id) as { next_token: string | null; complete: number } | undefined;
      if (!current || current.complete || current.next_token !== work.next_token) throw new Error("Matrix history continuation changed");
      if (this.db.prepare("SELECT 1 FROM pcp_matrix_history_pages WHERE id=?").get(id)) {
        if (page.next !== work.next_token) throw new Error("Retained Matrix history page has inconsistent continuation");
        this.markHistoryAttempt(work.id, Date.parse(observedAt), "stalled");
        return;
      }
      this.db.prepare(`INSERT INTO pcp_matrix_history_pages(id,job_id,from_token,next_token,sha256,observed_at,item_count)
        VALUES (?,?,?,?,?,?,?)`).run(id, work.id, work.next_token, page.next, hash, observedAt, page.items.length);
      for (const item of page.items) this.retainItem(item, id);
      const complete = page.next === null || work.stop_event_id !== null && page.items.some(item => item.eventId === work.stop_event_id);
      const stalled = !complete && page.next === work.next_token;
      this.db.prepare(`UPDATE pcp_matrix_history_jobs SET next_token=?,complete=?,pages=pages+1,status=?,retry_after_ms=?
        WHERE id=?`).run(page.next, complete ? 1 : 0, complete ? "complete" : stalled ? "stalled" : "pending",
        stalled ? Date.parse(observedAt) + 300_000 : 0, work.id);
    }).immediate();
  }

  hasLiveReady(): boolean {
    return !!this.db.prepare("SELECT 1 FROM pcp_matrix_observations WHERE mode='live' AND state='ready' LIMIT 1").get();
  }

  publicationWork(now = Date.now(), excluded: ReadonlySet<string> = new Set(), includeRecovery = true): { id: string; state: State; mode: AcquisitionMode } | null {
    this.assertBinding();
    const rows = this.db.prepare(`SELECT id,state,mode FROM pcp_matrix_observations WHERE state='ready'
      OR (?=1 AND (state='uncertain' AND last_attempt_ms<=? OR
        state='acknowledged' AND preparation_pending=1 AND preparation_retry_ms<=?))
      ORDER BY CASE WHEN state='ready' AND mode='live' THEN 0 WHEN state='uncertain' THEN 1
        WHEN state='ready' THEN 2 ELSE 3 END,last_attempt_ms,rowid LIMIT 50`)
      .all(includeRecovery ? 1 : 0, now - 60_000, now) as { id: string; state: State; mode: AcquisitionMode }[];
    return rows.find(row => !excluded.has(row.id)) ?? null;
  }

  markPublicationAttempt(id: string, now = Date.now()): void {
    this.db.prepare("UPDATE pcp_matrix_observations SET last_attempt_ms=? WHERE id=?").run(now, id);
  }

  mediaSpoolDirectory(): string | null {
    return isAbsolute(this.db.name) ? join(dirname(this.db.name), "pcp-media") : null;
  }

  publishedAttachments(id: string): Map<string, string> {
    const rows = this.db.prepare(`SELECT query_json FROM pcp_matrix_publication_archive WHERE id=?
      UNION ALL SELECT query_json FROM pcp_matrix_observations WHERE id=? AND state='acknowledged'`).all(id, id) as
      { query_json: string }[];
    const output = new Map<string, string>();
    for (const row of rows) for (const attachment of JSON.parse(row.query_json).attachments) output.set(attachment.locator_sha256, attachment.sha256);
    return output;
  }

  /** Before the first effect a prepared view can be frozen. After an exact
   * positive receipt, a different monotonic rendition gets a new archived
   * publication generation. Uncertain attempts are never changed or replayed. */
  freezePrepared(id: string, body: string, pending: boolean, now = Date.now()): void {
    if (!allJoinedSelection(this.selection) || Buffer.byteLength(body) > 24 * 1024 * 1024) throw new Error("Invalid prepared Matrix publication");
    this.db.transaction(() => {
      const current = this.publication(id);
      if (!current) throw new Error("Matrix publication missing");
      const row = this.db.prepare("SELECT state,attempt_count,receipt_json,generation FROM pcp_matrix_observations WHERE id=?").get(id) as
        { state: State; attempt_count: number; receipt_json: string | null; generation: number };
      if (row.state !== "ready" && row.state !== "acknowledged" || row.state === "ready" && row.attempt_count !== 0) {
        throw new Error("Attempted Matrix publication cannot be changed");
      }
      const before = JSON.parse(current.body), after = JSON.parse(body);
      if (canonicalJson(before.observation.raw) !== canonicalJson(after.observation.raw) ||
          before.provider_bytes !== after.provider_bytes ||
          canonicalJson(publicationContext(before, this.selection)) !== canonicalJson(publicationContext(after, this.selection)) ||
          before.observation.decrypted_event && canonicalJson(before.observation.decrypted_event) !== canonicalJson(after.observation.decrypted_event)) {
        throw new Error("Prepared Matrix view changed its retained original");
      }
      const query = JSON.stringify(publicationQuery(body, this.selection)), queryDigest = digest(canonicalJson(JSON.parse(query)));
      if (row.state === "acknowledged" && queryDigest !== current.queryDigest) {
        if (!row.receipt_json || !exactPositiveReceipt(current, this.selection, JSON.parse(row.receipt_json))) throw new Error("Prior Matrix rendition receipt unavailable");
        this.db.prepare(`INSERT INTO pcp_matrix_publication_archive(id,generation,publication_json,query_json,query_digest,receipt_json)
          VALUES (?,?,?,?,?,?)`).run(id, row.generation, current.body, current.query, current.queryDigest, row.receipt_json);
        this.db.prepare(`UPDATE pcp_matrix_observations SET generation=generation+1,state='ready',attempt_count=0,receipt_json=NULL WHERE id=?`).run(id);
      }
      if (row.state !== "acknowledged" || queryDigest !== current.queryDigest) {
        this.db.prepare("UPDATE pcp_matrix_observations SET publication_json=?,query_json=?,query_digest=?,gaps_json=? WHERE id=?")
          .run(body, query, queryDigest, JSON.stringify(after.observation.gaps), id);
      }
      this.db.prepare("UPDATE pcp_matrix_observations SET preparation_pending=?,preparation_retry_ms=? WHERE id=?")
        .run(pending ? 1 : 0, now + 300_000, id);
    }).immediate();
  }

  holdDecryptedMutation(id: string, target: string | null): void {
    const original = this.publication(id);
    if (!original) return;
    const { room } = publicationContext(JSON.parse(original.body), this.selection);
    this.db.prepare("UPDATE pcp_matrix_observations SET state='held',target_event_id=?,gaps_json=? WHERE id=? AND state='ready'")
      .run(target, JSON.stringify(["lifecycle_contract_pending"]), id);
    if (target) this.db.prepare("UPDATE pcp_matrix_observations SET state='held' WHERE room_id=? AND event_id=? AND state='ready'")
      .run(room, target);
  }

  coverage(): { rooms: number; historicalRooms: number; unfinishedHistory: number; unfinishedState: number; accessibleSnapshots: number; pendingPublications: number; heldObservations: number } {
    const count = (query: string) => (this.db.prepare(query).get() as { n: number }).n;
    return {
      rooms: count("SELECT count(*) n FROM pcp_matrix_rooms WHERE joined=1"),
      historicalRooms: count("SELECT count(*) n FROM pcp_matrix_rooms WHERE joined=0"),
      unfinishedHistory: count("SELECT count(*) n FROM pcp_matrix_history_jobs WHERE kind<>'state' AND complete=0"),
      unfinishedState: count("SELECT count(*) n FROM pcp_matrix_history_jobs WHERE kind='state' AND complete=0"),
      accessibleSnapshots: count("SELECT count(*) n FROM pcp_matrix_accessible_snapshots"),
      pendingPublications: count("SELECT count(*) n FROM pcp_matrix_observations WHERE state IN ('ready','uncertain') OR preparation_pending=1 AND state='acknowledged'"),
      heldObservations: count("SELECT count(*) n FROM pcp_matrix_observations WHERE state='held'"),
    };
  }

  obligations(limit = 100): PcpObligation[] {
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 1000) throw new Error("Invalid obligation bound");
    const rows = this.db.prepare("SELECT id,event_id,state,gaps_json FROM pcp_matrix_observations ORDER BY rowid LIMIT ?")
      .all(limit) as { id: string; event_id: string | null; state: State; gaps_json: string }[];
    return rows.map(({ gaps_json, ...row }) => ({ ...row, gaps: JSON.parse(gaps_json) }));
  }

  /** Exact local readback is source-scoped and never mutates a responsibility. */
  original(id: string): Buffer | null {
    this.assertBinding();
    const row = this.db.prepare("SELECT wire,wire_sha256 FROM pcp_matrix_observations WHERE id=?")
      .get(id) as { wire: Buffer; wire_sha256: string } | undefined;
    if (!row) return null;
    if (digest(row.wire) !== row.wire_sha256) throw new Error("Retained Matrix observation failed digest check");
    return Buffer.from(row.wire);
  }

  publication(id: string): FrozenPublication | null {
    this.assertBinding();
    const row = this.db.prepare(
      "SELECT publication_json,query_json,query_digest FROM pcp_matrix_observations WHERE id=?")
      .get(id) as { publication_json: string | null; query_json: string | null; query_digest: string | null } | undefined;
    if (!row?.publication_json || !row.query_json || !row.query_digest) return null;
    const query = publicationQuery(row.publication_json, this.selection);
    const wire = this.original(id);
    if (!wire) throw new Error("Retained Matrix observation unavailable");
    const body = JSON.parse(row.publication_json);
    const native = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(wire));
    const expected = body.provider_bytes === undefined ? { ...native, room_id: publicationContext(body, this.selection).room } : native;
    if (canonicalJson(query) !== canonicalJson(JSON.parse(row.query_json)) ||
        digest(canonicalJson(query)) !== row.query_digest ||
        canonicalJson(body.observation.raw) !== canonicalJson(expected) ||
        body.provider_bytes !== undefined && body.provider_bytes !== wire.toString("base64")) {
      throw new Error("Frozen Matrix publication failed integrity check");
    }
    return { id, body: row.publication_json, query: row.query_json, queryDigest: row.query_digest };
  }

  /** Commit uncertainty BEFORE the first possible remote effect. A crash here
   * may strand a never-sent item; it cannot authorize an accidental second POST. */
  claim(id: string): FrozenPublication | null {
    return this.db.transaction(() => {
      const frozen = this.publication(id);
      if (!frozen) return null;
      const result = this.db.prepare(`UPDATE pcp_matrix_observations
        SET state='uncertain',attempt_count=attempt_count+1 WHERE id=? AND state='ready' AND attempt_count=0`).run(id);
      return result.changes === 1 ? frozen : null;
    })();
  }

  acknowledge(id: string, receipt: unknown): boolean {
    this.assertBinding();
    const frozen = this.publication(id);
    if (!frozen || !exactPositiveReceipt(frozen, this.selection, receipt)) return false;
    return this.db.prepare(`UPDATE pcp_matrix_observations SET state='acknowledged',receipt_json=?
      WHERE id=? AND query_digest=? AND state='uncertain' AND attempt_count=1`)
      .run(JSON.stringify(receipt), id, frozen.queryDigest).changes === 1;
  }

  state(id: string): State | null {
    const row = this.db.prepare("SELECT state FROM pcp_matrix_observations WHERE id=?").get(id) as
      { state: State } | undefined;
    return row?.state ?? null;
  }
}

/** The locked SDK's first request reads getSavedSyncToken, not getSyncToken.
 * Reuse its normal in-memory room store while reading the sole durable cursor.
 * No saved room snapshot or upstream-history coverage is invented. */
export class PcpSyncMemoryStore extends MemoryStore {
  constructor(private readonly retained: () => PcpSyncHandoff | undefined) { super(); }

  override async getSavedSyncToken(): Promise<string | null> {
    const handoff = this.retained();
    if (!handoff) throw new Error("Matrix PCP checkpoint is not bound before SDK startup");
    const checkpoint = handoff.checkpoint();
    if (checkpoint !== null) this.setSyncToken(checkpoint);
    return checkpoint;
  }
}

function syncRequest(input: Parameters<typeof fetch>[0], init: Parameters<typeof fetch>[1], selection: PcpSyncSelection) {
  const url = new URL(typeof input === "string" ? input : input instanceof URL ? input.href : input.url);
  if (!/^\/_matrix\/client\/(?:v3|r0)\/sync$/.test(url.pathname)) return null;
  if (url.origin !== selection.homeserverUrl || url.username || url.password || url.hash ||
      (init?.method ?? (typeof input === "object" && "method" in input ? input.method : "GET")).toUpperCase() !== "GET" ||
      url.searchParams.getAll("since").length > 1) throw new Error("Matrix sync differs from selected transport");
  const since = url.searchParams.get("since");
  if (since !== null && (!since || since.length > 16_384)) throw new Error("Invalid Matrix sync checkpoint");
  return { since };
}

/** node-fetch clone streams can deadlock on large unconsumed responses. Consume
 * one bounded body, commit it, then provide those same bytes to the locked SDK. */
export function retainSyncFetch(base: typeof fetch, handoff: PcpSyncHandoff): typeof fetch {
  return (async (input, init) => {
    const request = syncRequest(input, init, handoff.selection);
    const response = await base(input, init);
    if (!request || !response.ok) return response;
    if (!response.body) throw new Error("Matrix sync body unavailable");
    const chunks: Buffer[] = [];
    let size = 0;
    for await (const chunk of response.body) {
      const bytes = Buffer.from(chunk);
      size += bytes.length;
      if (size > MAX_SYNC_BYTES) {
        (response.body as Readable).destroy();
        throw new Error("Matrix sync exceeds retention bound");
      }
      chunks.push(bytes);
    }
    const bytes = Buffer.concat(chunks, size);
    handoff.retain(bytes, request.since);
    const properties = {
      status: response.status, statusText: response.statusText, headers: response.headers,
      url: response.url,
    };
    return new Response(bytes, properties);
  }) as typeof fetch;
}
