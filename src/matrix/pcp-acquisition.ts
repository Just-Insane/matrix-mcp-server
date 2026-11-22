import fetch from "node-fetch";
import type { Readable } from "node:stream";
import type { MatrixClient } from "matrix-js-sdk";
import { assertNativeSelection, MAX_SYNC_BYTES, MAX_SNAPSHOT_BYTES, type PcpSyncHandoff, type PcpHistoryWork } from "./pcp-sync-handoff.js";
import { allJoinedSelection, canonicalJson, digest, object, publicationContext, observationItemId } from "./pcp-sync-observation.js";
import { PcpReceiver, publishPcpObservation } from "./pcp-publication.js";
import { boundedRequestSignal } from "./request-signal.js";
import { PcpMediaTransfers, decryptedMediaLocator, type MediaInfo, type MediaContext, type PreparedMedia, type MediaOriginal } from "./pcp-media.js";

const MEDIA_BYTES = 16 * 1024 * 1024;
const ACQUISITION_GAPS = new Set(["decryption_unavailable", "attachment_inventory_unavailable", "attachment_bytes_unavailable",
  "attachment_size_limit", "attachment_platform_size_limit", "attachment_locator_unsupported", "attachment_download_unavailable", "attachment_decryption_unavailable"]);
export type AcquisitionClient = Pick<MatrixClient, "getJoinedRooms" | "getUserId" | "getDeviceId" | "getHomeserverUrl" |
  "getAccessToken" | "getEventMapper" | "decryptEventIfNeeded" | "mxcUrlToHttp" | "getSyncState">;
class NativeReadUnavailable extends Error {
  constructor(readonly status: number) { super("Matrix native read unavailable"); }
}

/** Read-only requests using the same retained native identity. An initial
 * inventory snapshot is independent of the one live /sync cursor and never
 * enters the SDK. No timeline mutation, key restore, membership or new client. */
export class PcpNativeReader {
  private mediaTransfers?: PcpMediaTransfers;
  constructor(private readonly client: AcquisitionClient, private readonly handoff: PcpSyncHandoff,
    private readonly fetchFn: typeof fetch = fetch) {
    assertNativeSelection(handoff.selection, { homeserverUrl: client.getHomeserverUrl(),
      userId: client.getUserId(), deviceId: client.getDeviceId() });
  }

  private async bytes(url: URL, maximum: number): Promise<Buffer> {
    if (url.origin !== this.handoff.selection.homeserverUrl || url.username || url.password || url.hash) {
      throw new Error("Matrix native read differs from retained homeserver");
    }
    const token = this.client.getAccessToken();
    if (!token) throw new Error("Retained Matrix credential unavailable");
    const deadline = boundedRequestSignal(undefined, 15_000);
    try {
      const response = await this.fetchFn(url, { method: "GET", redirect: "error", signal: deadline.signal,
        headers: { Authorization: "Bearer " + token } });
      if (!response.ok || !response.body) {
        (response.body as Readable | null)?.destroy(); throw new NativeReadUnavailable(response.status);
      }
      const chunks: Buffer[] = [];
      let size = 0;
      for await (const chunk of response.body) {
        const bytes = Buffer.from(chunk); size += bytes.length;
        if (size > maximum) {
          (response.body as Readable).destroy(); throw new Error("Matrix native object exceeds retained byte budget");
        }
        chunks.push(bytes);
      }
      return Buffer.concat(chunks, size);
    } finally { deadline.dispose(); }
  }

  history(work: PcpHistoryWork): Promise<Buffer> {
    const url = new URL("/_matrix/client/v3/rooms/" + encodeURIComponent(work.room_id) + (work.kind === "state" ? "/state" : "/messages"), this.handoff.selection.homeserverUrl);
    if (work.kind === "state") return this.bytes(url, MAX_SNAPSHOT_BYTES);
    url.searchParams.set("dir", "b"); url.searchParams.set("limit", "100");
    if (work.next_token !== null) url.searchParams.set("from", work.next_token);
    return this.bytes(url, MAX_SYNC_BYTES);
  }

  accessibleInventory(): Promise<Buffer> {
    const url = new URL("/_matrix/client/v3/sync", this.handoff.selection.homeserverUrl);
    url.searchParams.set("timeout", "0");
    url.searchParams.set("filter", JSON.stringify({ room: { include_leave: true, timeline: { limit: 1 },
      state: { lazy_load_members: true }, ephemeral: { types: [] }, account_data: { types: [] } },
      presence: { types: [] }, account_data: { types: [] } }));
    // This bounded inventory avoids one huge repeated member list. Separate
    // /state jobs retain the full accessible state, including every member.
    return this.bytes(url, MAX_SNAPSHOT_BYTES);
  }

  async preparedMedia(context: MediaContext, info: MediaInfo, receiver: PcpReceiver): Promise<PreparedMedia> {
    if (receiver.selectionDigest !== this.handoff.selectionDigest) throw new Error("Matrix media receiver differs");
    const root = this.handoff.mediaSpoolDirectory();
    if (!root) return { parts: [], pending: true, gaps: ["attachment_bytes_unavailable"] };
    this.mediaTransfers ??= new PcpMediaTransfers(root, this.handoff.selectionDigest,
      async (locator, signal) => {
        const url = this.mediaUrl(locator), token = this.client.getAccessToken();
        if (!token) throw new Error("Retained Matrix credential unavailable");
        const read = () => this.fetchFn(url, { method: "GET", redirect: "error", signal,
          headers: { Authorization: "Bearer " + token } });
        let response = await read();
        if ([404, 405].includes(response.status)) {
          (response.body as Readable | null)?.destroy(); url.pathname = url.pathname.replace("/_matrix/client/v1/media/download/", "/_matrix/media/v3/download/");
          response = await read();
        }
        return response;
      }, (context, locator, file, signal, canContinue) => receiver.uploadOriginal(context, locator, file, signal, canContinue),
      () => !this.handoff.hasLiveReady());
    return this.mediaTransfers.prepare(context, info);
  }

  stopMedia(): void { this.mediaTransfers?.stop(); }

  private mediaUrl(locator: string): URL {
    if (!locator.startsWith("mxc://")) throw new Error("Unsupported Matrix media locator");
    const candidate = this.client.mxcUrlToHttp(locator, undefined, undefined, undefined, false, false, true);
    if (!candidate) throw new Error("Unsupported Matrix media locator");
    const url = new URL(candidate);
    if (url.origin !== this.handoff.selection.homeserverUrl || url.username || url.password || url.hash ||
        !url.pathname.startsWith("/_matrix/client/v1/media/download/")) throw new Error("Unsupported authenticated Matrix homeserver media route");
    url.searchParams.set("allow_redirect", "false"); return url;
  }

  async media(locator: string): Promise<Buffer> {
    // External content URLs remain references. Native MXC downloads stay on the
    // authenticated homeserver and cannot redirect credentials to an event URL.
    const url = this.mediaUrl(locator);
    try { return await this.bytes(url, MEDIA_BYTES); }
    catch (error) {
      if (!(error instanceof NativeReadUnavailable) || ![404, 405].includes(error.status)) throw error;
      // Older native homeservers expose the SDK's legacy media route. Preserve
      // the same authenticated origin, byte bound and redirect prohibition.
      url.pathname = url.pathname.replace("/_matrix/client/v1/media/download/", "/_matrix/media/v3/download/");
      return this.bytes(url, MEDIA_BYTES);
    }
  }
}

function mediaInventory(content: any): MediaInfo[] {
  const candidates: { locator: any; file: any; size: any; mediaType: any }[] = [];
  const visit = (value: any, depth = 0) => {
    if (!value || typeof value !== "object" || depth > 64) return;
    if (typeof value.url === "string") candidates.push({ locator: value.url,
      file: value.key && value.iv ? value : null, size: value.info?.size, mediaType: value.info?.mimetype });
    if (value.file?.url) candidates.push({ locator: value.file.url, file: value.file, size: value.info?.size, mediaType: value.info?.mimetype });
    if (value.thumbnail_url) candidates.push({ locator: value.thumbnail_url, file: null, size: value.thumbnail_info?.size, mediaType: value.thumbnail_info?.mimetype });
    if (value.thumbnail_file?.url) candidates.push({ locator: value.thumbnail_file.url, file: value.thumbnail_file, size: value.thumbnail_info?.size, mediaType: value.thumbnail_info?.mimetype });
    for (const [key, child] of Object.entries(value)) if (!["file", "thumbnail_file"].includes(key)) visit(child, depth + 1);
  };
  visit(content);
  const result = new Map<string, ReturnType<typeof mediaInventory>[number]>();
  for (const entry of candidates) if (typeof entry.locator === "string" && entry.locator.length && entry.locator.length <= 2048) {
    const hash = entry.file?.hashes?.sha256;
    const decoded = typeof hash === "string" ? Buffer.from(hash, "base64") : null;
    result.set(entry.locator, { locator: entry.locator, encrypted: !!entry.file,
      expectedDigest: decoded?.length === 32 ? decoded.toString("hex") : null,
      expectedSize: Number.isSafeInteger(entry.size) && entry.size >= 0 ? entry.size : null,
      file: entry.file, mediaType: entry.mediaType });
  }
  return [...result.values()];
}

/** Use the existing crypto context for the event's original content, never a
 * bundled replacement applied to another event.
 * Partial key/media availability leaves a durable retry and the full original
 * independently publishable. Exact ciphertext media is never relabelled clear. */
export async function preparePcpPublication(handoff: PcpSyncHandoff, id: string, client: AcquisitionClient,
  reader: Pick<PcpNativeReader, "media"> & Partial<Pick<PcpNativeReader, "preparedMedia">>, now = Date.now(), receiver?: PcpReceiver): Promise<void> {
  const state = handoff.state(id), frozen = handoff.publication(id);
  if (!frozen || state !== "ready" && state !== "acknowledged") return;
  const before = JSON.parse(frozen.body), body = JSON.parse(frozen.body), observation = body.observation;
  const context = publicationContext(body, handoff.selection);
  const gaps = new Set<string>((observation.gaps as string[]).filter(gap => !ACQUISITION_GAPS.has(gap)));
  let content = observation.raw.content, decryptionMissing = false;
  if (observation.raw.type === "m.room.encrypted") {
    if (!observation.decrypted_event) {
      const event = client.getEventMapper()({ ...JSON.parse(JSON.stringify(observation.raw)), room_id: context.room });
      let timeout: ReturnType<typeof setTimeout> | undefined;
      try {
        await Promise.race([client.decryptEventIfNeeded(event), new Promise<void>((_resolve, reject) => {
          timeout = setTimeout(() => reject(new Error("Existing Matrix key unavailable")), 10_000);
        })]);
        if (event.isDecryptionFailure() || event.getType() === "m.room.encrypted") throw new Error("Existing Matrix key unavailable");
        const clear = JSON.parse(JSON.stringify(event.getOriginalContent()));
        if (event.isRedacted()) throw new Error("Native event content was redacted");
        observation.decrypted_event = { room_id: context.room, event_id: observation.event_id, type: event.getType(), content: clear };
      } catch { decryptionMissing = true; }
      finally { if (timeout) clearTimeout(timeout); }
    }
    if (observation.decrypted_event) {
      content = observation.decrypted_event.content;
      observation.decryption = "available";
      const relation = content?.["m.relates_to"];
      const edit = relation?.rel_type === "m.replace", redaction = observation.decrypted_event.type === "m.room.redaction";
      if (edit || redaction) {
        const target = edit ? relation.event_id : content.redacts;
        const known = typeof target === "string" && target.length > 0 && target.length <= 2048 && !/[\u0000-\u001f\u007f]/.test(target);
        observation.relation = { kind: known ? edit ? "edit" : "redaction" : "unknown", target_event_id: known ? target : null };
        if (!known) gaps.add("native_relation_target_unavailable");
      }
      if (!observation.thread_id && ["m.thread", "io.element.thread"].includes(relation?.rel_type) && typeof relation.event_id === "string") {
        observation.thread_id = relation.event_id;
      }
    } else {
      observation.decryption = "missing";
      gaps.add("decryption_unavailable"); gaps.add("attachment_inventory_unavailable");
    }
  }
  const inventory = mediaInventory(content);
  const known = new Set<string>(observation.attachments.map((item: any) => item.locator));
  for (const item of inventory) if (!known.has(item.locator)) {
    if (observation.attachments.length >= 32) throw new Error("Matrix attachment inventory exceeds receiver bound");
    observation.attachments.push({ locator: item.locator, custody: "reference" }); known.add(item.locator);
  }
  const published = handoff.publishedAttachments(id);
  const attachments: { locator: string; base64?: string; sha256: string; original?: MediaOriginal }[] = state === "ready" ? body.attachments ?? [] : [];
  let bytes = attachments.reduce((total, item) => total + (item.base64 ? Buffer.from(item.base64, "base64").length : 0), 0);
  let downloads = 0, pending = decryptionMissing;
  const captured = new Set(attachments.map(item => item.locator));
  const streamed = async (info: MediaInfo): Promise<boolean> => {
    if (!reader.preparedMedia || !receiver) return false;
    const result = await reader.preparedMedia({ ...context, itemId: observationItemId(observation) }, info, receiver);
    pending ||= result.pending; for (const gap of result.gaps) gaps.add(gap);
    for (const part of result.parts) {
      if (!known.has(part.locator)) {
        observation.attachments.push({ locator: part.locator, custody: "reference", ...(part.derivation ? { derivation: part.derivation } : {}) }); known.add(part.locator);
      }
      if (part.derivation) gaps.delete("attachment_decryption_unavailable");
      if (!captured.has(part.locator) && !published.has(digest(part.locator))) {
        attachments.push({ locator: part.locator, sha256: part.original.sha256, original: part.original }); captured.add(part.locator);
      }
    }
    return true;
  };
  for (const reference of [...observation.attachments]) {
    const info = inventory.find(item => item.locator === reference.locator);
    if (info?.encrypted) gaps.add("attachment_decryption_unavailable");
    if (reference.derivation?.kind === "matrix-media-decryption") continue;
    const cipherSha = published.get(digest(reference.locator)) ?? info?.expectedDigest;
    const decodedKnown = !!cipherSha && (published.has(digest(decryptedMediaLocator(reference.locator, cipherSha))) || captured.has(decryptedMediaLocator(reference.locator, cipherSha)));
    if (decodedKnown) gaps.delete("attachment_decryption_unavailable");
    if ((published.has(digest(reference.locator)) || captured.has(reference.locator)) && (!info?.encrypted || decodedKnown)) continue;
    gaps.add("attachment_bytes_unavailable");
    if (!reference.locator.startsWith("mxc://")) { gaps.add("attachment_locator_unsupported"); continue; }
    if (info && (info.encrypted || info.expectedSize !== null && info.expectedSize > MEDIA_BYTES) && await streamed(info)) continue;
    if (info?.expectedSize !== null && info?.expectedSize !== undefined && info.expectedSize > MEDIA_BYTES) {
      gaps.add("attachment_size_limit"); continue;
    }
    if (downloads >= 4 || bytes >= MEDIA_BYTES) { pending = true; continue; }
    downloads++;
    try {
      const media = await reader.media(reference.locator);
      if (media.length > MEDIA_BYTES) { gaps.add("attachment_size_limit"); continue; }
      if (bytes + media.length > MEDIA_BYTES) { pending = true; continue; }
      const sha256 = digest(media);
      if (info?.expectedDigest && sha256 !== info.expectedDigest) throw new Error("Matrix media digest differs");
      attachments.push({ locator: reference.locator, base64: media.toString("base64"), sha256 });
      captured.add(reference.locator); bytes += media.length;
    } catch (error) {
      if (error instanceof Error && /byte budget/.test(error.message)) {
        if (!await streamed(info ?? { locator: reference.locator, encrypted: false, expectedDigest: null, expectedSize: null })) gaps.add("attachment_size_limit");
      }
      else { gaps.add("attachment_download_unavailable"); pending = true; }
    }
  }
  if (observation.attachments.every((item: any) => captured.has(item.locator) || published.has(digest(item.locator)))) {
    gaps.delete("attachment_bytes_unavailable");
  }
  observation.gaps = [...gaps];
  if (attachments.length) body.attachments = attachments; else delete body.attachments;
  // Removing a previously submitted byte batch alone is not a new rendition.
  // Its exact successful publication stays archived only when new evidence exists.
  const changed = canonicalJson(before.observation) !== canonicalJson(observation) || attachments.length > 0 && state === "acknowledged";
  handoff.freezePrepared(id, state === "acknowledged" && !changed ? frozen.body : JSON.stringify(body), pending, now);
}

export async function runPcpAcquisitionTick({ client, handoff, receiver, reader, now = Date.now, stopped = () => false }: {
  client: AcquisitionClient; handoff: PcpSyncHandoff; receiver: PcpReceiver; reader: PcpNativeReader;
  now?: () => number; stopped?: () => boolean;
}): Promise<void> {
  if (!allJoinedSelection(handoff.selection) || stopped()) return;
  const deadline = now() + 45_000, visited = new Set<string>();
  let recovery = 0;
  // Fresh live custody is selected again between every remote operation; a
  // historical batch cannot pre-fill the publisher ahead of a new live event.
  for (let count = 0; count < 10 && now() < deadline && !stopped(); count++) {
    const item = handoff.publicationWork(now(), visited, recovery < 2);
    if (!item) break;
    visited.add(item.id); handoff.markPublicationAttempt(item.id, now());
    if (item.state !== "ready") recovery++;
    try {
      if (item.state !== "uncertain") await preparePcpPublication(handoff, item.id, client, reader, now(), receiver);
      if (!stopped()) await publishPcpObservation(handoff, item.id, receiver);
    } catch { /* Independent durable obligation stays visible for recovery. */ }
  }
  if (stopped() || now() >= deadline || handoff.hasLiveReady()) return;
  // Discovery enumerates the complete joined-room set each turn, including
  // quiet rooms absent from a resumed sync. It does not open another sync loop.
  try {
    const inventory = await client.getJoinedRooms();
    handoff.recordJoinedRooms(inventory.joined_rooms);
  } catch { return; }
  if (stopped() || handoff.hasLiveReady() || now() >= deadline) return;
  const coverage = handoff.coverage();
  if (coverage.pendingPublications >= 200) return;
  if (handoff.accessibleInventoryDue(now())) {
    handoff.markAccessibleInventory(now());
    try { handoff.retainAccessibleInventory(await reader.accessibleInventory(), new Date(now()).toISOString()); }
    catch { handoff.markAccessibleInventory(now(), "unavailable"); }
    return;
  }
  const work = handoff.historyWork(now());
  if (!work) return;
  handoff.markHistoryAttempt(work.id, now());
  try {
    const bytes = await reader.history(work);
    // Completed read custody is durable even if live work appeared during I/O.
    // Publication of these historical events waits behind the live lane.
    handoff.retainHistory(work, bytes, new Date(now()).toISOString());
  } catch { handoff.markHistoryAttempt(work.id, now(), "unavailable"); }
}

/** The existing collector owns this facet's lifetime and single-flight guard.
 * No new host service, cron, SDK client or /sync consumer is introduced. */
export function startPcpAcquisition(client: AcquisitionClient, handoff: PcpSyncHandoff, receiver: PcpReceiver): () => void {
  const reader = new PcpNativeReader(client, handoff);
  let stopped = false, running = false;
  const tick = async () => {
    if (stopped || running) return;
    running = true;
    try {
      const state = client.getSyncState();
      if (state !== null && state !== "ERROR" && state !== "STOPPED") await runPcpAcquisitionTick({
        client, handoff, receiver, reader, stopped: () => stopped,
      });
    } catch { console.error("[pcp-acquisition] Retained work remains pending"); }
    finally { running = false; }
  };
  const interval = setInterval(() => { void tick(); }, 30_000);
  void tick();
  return () => { stopped = true; clearInterval(interval); reader.stopMedia(); };
}
