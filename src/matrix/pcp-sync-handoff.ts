import type Database from "better-sqlite3";
import type fetch from "node-fetch";
import { Response } from "node-fetch";
import type { Readable } from "node:stream";
import { MemoryStore } from "matrix-js-sdk";
import { exactPositiveReceipt, publicationQuery } from "./pcp-publication.js";
import {
  canonicalJson, digest, freezeSelection, selectSync,
  type PcpSyncSelection,
} from "./pcp-sync-observation.js";

export const PCP_SELECTION_KEY = "pcp_matrix_selection";
export const PCP_START_BOUNDARY_KEY = "pcp_matrix_start_boundary";
export const MAX_SYNC_BYTES = 8 * 1024 * 1024;
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
    const selected = selectSync(bytes, this.selection, observedAt);
    const batchId = digest(canonicalJson([this.selectionDigest, since, selected.nextBatch]));
    const selectedDigest = digest(canonicalJson({
      items: selected.items.map(item => [item.wireSha256, item.section, item.stream, item.ordinal]),
      coverage: selected.coverage,
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
      this.db.prepare("INSERT OR REPLACE INTO sync_state(key,value) VALUES ('sync_token',?)")
        .run(selected.nextBatch);
      return batchId;
    })();
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
    const expected = body.provider_bytes === undefined ? { ...native, room_id: this.selection.policy.room } : native;
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
