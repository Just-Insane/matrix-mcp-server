import fetch from "node-fetch";
import type { DecisionRoomsSnapshot } from "./pcp-decision-rooms.js";
import type { Readable } from "node:stream";
import { constants } from "node:fs";
import { open } from "node:fs/promises";
import { boundedRequestSignal } from "./request-signal.js";
import { MEDIA_CHUNK_BYTES, MAX_MEDIA_ORIGINAL_BYTES, type MediaOriginal, type MediaContext, type SpooledMedia } from "./pcp-media.js";
import type { PcpSyncHandoff, FrozenPublication } from "./pcp-sync-handoff.js";
import { canonicalJson, digest, freezeSelection, object, publicationContext, allJoinedSelection, observationItemId, sourceObservationId, type PcpSyncSelection, type AcquisitionMode } from "./pcp-sync-observation.js";

const text = (value: unknown): value is string =>
  typeof value === "string" && value.length > 0 && value.length <= 2048 && !/[\u0000-\u001f\u007f]/.test(value);

export function publicationQuery(body: string, selection: PcpSyncSelection): Record<string, unknown> {
  const value = JSON.parse(body), observation = value?.observation;
  const context = publicationContext(value, selection);
  const sourceObservation = observation?.schema === "matrix-discord-observation/v2";
  const stripped = sourceObservation && observation.provenance?.kind === "stripped_state";
  if (!object(observation) || !["matrix-discord-observation/v1", "matrix-discord-observation/v2"].includes(String(observation.schema)) ||
      observation.source !== "matrix" || observation.account !== selection.policy.account ||
      observation.room_id !== context.room || !object(observation.raw) ||
      (stripped ? observation.event_id !== null || observation.raw.event_id !== undefined
        : !text(observation.event_id) || observation.raw.event_id !== observation.event_id) ||
      (stripped ? observation.raw.room_id !== undefined && observation.raw.room_id !== context.room : observation.raw.room_id !== context.room) ||
      !allJoinedSelection(selection) && (value.attachments !== undefined || sourceObservation)) {
    throw new Error("Frozen Matrix publication differs from selected source");
  }
  if (sourceObservation) {
    const p = observation.provenance;
    if (!object(p) || Object.keys(p).sort().join() !== "endpoint,kind,section,stream" ||
        !["stripped_state", "native_event_view"].includes(String(p.kind)) ||
        !["sync", "messages", "state"].includes(String(p.endpoint)) || !text(p.section) || !text(p.stream) ||
        stripped && !(p.endpoint === "sync" && ((p.section === "invite" && p.stream === "invite_state") ||
          (p.section === "knock" && p.stream === "knock_state"))) ||
        observation.observation_id !== sourceObservationId(context.room, String(p.kind), observation.raw, p.section, p.stream)) {
      throw new Error("Invalid frozen Matrix source observation");
    }
  } else if (observation.observation_id !== undefined || observation.provenance !== undefined) throw new Error("Unexpected source observation identity");
  let native: Buffer;
  const fidelity = value.provider_bytes === undefined ? "transformed" : "original";
  if (fidelity === "original") {
    if (typeof value.provider_bytes !== "string") throw new Error("Invalid frozen Matrix original");
    native = Buffer.from(value.provider_bytes, "base64");
    if (native.toString("base64") !== value.provider_bytes ||
        canonicalJson(JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(native))) !== canonicalJson(observation.raw)) {
      throw new Error("Frozen Matrix original differs from observation");
    }
  } else native = Buffer.from(JSON.stringify(observation.raw));
  const { observed_at: _clock, ...meaning } = observation;
  const attachments: { locator_sha256: string; sha256: string }[] = [];
  let attachmentBytes = 0;
  if (value.attachments !== undefined) {
    if (!Array.isArray(value.attachments) || value.attachments.length > 32) throw new Error("Invalid Matrix attachment publication");
    for (const attachment of value.attachments) {
      if (!object(attachment) || !["base64,locator,sha256", "locator,original,sha256"].includes(Object.keys(attachment).sort().join()) ||
          !text(attachment.locator) || !/^[a-f0-9]{64}$/.test(String(attachment.sha256)) ||
          !Array.isArray(observation.attachments) || !observation.attachments.some(item => object(item) && item.locator === attachment.locator)) {
        throw new Error("Invalid frozen Matrix attachment");
      }
      if (attachment.original !== undefined) {
        const original = attachment.original;
        const itemId = canonicalJson([selection.policy.profile, context.room, observationItemId(observation)]);
        const scope = digest(canonicalJson(["ea-original-upload-v1", ["matrix", selection.policy.account, itemId, digest(attachment.locator)]]));
        if (!object(original) || Object.keys(original).sort().join() !== "byte_size,media_type,object_ref,sha256,source_locator" ||
            original.sha256 !== attachment.sha256 || original.source_locator !== attachment.locator ||
            !Number.isSafeInteger(original.byte_size) || Number(original.byte_size) < 1 || Number(original.byte_size) > MAX_MEDIA_ORIGINAL_BYTES ||
            typeof original.media_type !== "string" || !/^[\x21-\x7e]{1,128}$/.test(original.media_type) ||
            original.object_ref !== "r2://ea-raw/sources/common/streamed-originals/v1/" + scope + "/" + attachment.sha256) {
          throw new Error("Frozen Matrix original is not bound to its native item and locator");
        }
      } else {
        if (typeof attachment.base64 !== "string") throw new Error("Invalid frozen Matrix attachment bytes");
        const bytes = Buffer.from(attachment.base64, "base64"); attachmentBytes += bytes.length;
        if (bytes.toString("base64") !== attachment.base64 || digest(bytes) !== attachment.sha256 ||
            attachmentBytes > 16 * 1024 * 1024) throw new Error("Matrix attachment bytes differ or exceed bound");
      }
      attachments.push({ locator_sha256: digest(attachment.locator), sha256: String(attachment.sha256) });
    }
    if (new Set(attachments.map(item => item.locator_sha256)).size !== attachments.length) throw new Error("Duplicate Matrix attachment");
    attachments.sort((left, right) => left.locator_sha256 < right.locator_sha256 ? -1 : 1);
  }
  return {
    schema: sourceObservation ? "ea-chat-receipt-query-v2" : "ea-chat-receipt-query-v1", event_id: observation.event_id,
    ...(sourceObservation ? { observation_id: observation.observation_id } : {}),
    native_sha256: digest(native), observation_digest: digest(canonicalJson(meaning)),
    fidelity, attachments,
  };
}

/** The established receiver proves these identities and the exact query. An
 * HTTP success, capture response, generic receipt or absence cannot discharge. */
export function exactPositiveReceipt(
  frozen: FrozenPublication, selection: PcpSyncSelection, receipt: unknown,
): boolean {
  try {
    if (!object(receipt)) return false;
    const query = publicationQuery(frozen.body, selection);
    if (canonicalJson(query) !== canonicalJson(JSON.parse(frozen.query)) ||
        digest(canonicalJson(query)) !== frozen.queryDigest) return false;
    const item = canonicalJson([selection.policy.profile, publicationContext(JSON.parse(frozen.body), selection).room, observationItemId(query)]);
    const record = "ea-record:source-v2:" + digest(JSON.stringify(["matrix", selection.policy.account, item]));
    const revision = "source:" + digest(JSON.stringify(["matrix", selection.policy.account, item, "sha256:" + query.native_sha256]));
    const capture = "ea-capture:" + digest(JSON.stringify(["ea-intake-v1", record, revision]));
    const ref = receipt.recordReference;
    if (!object(ref) || !text(ref.representation_id) || !/^sha256:[a-f0-9]{64}$/.test(ref.representation_id)) return false;
    const expectedRef = "r2://ea-raw/sources/common/v2/" + digest(canonicalJson([record, revision])) +
      "/representations/" + ref.representation_id.slice(7) + ".json";
    return receipt.status === "captured" && receipt.read_only === true && receipt.replay_authorized === false &&
      receipt.query_digest === frozen.queryDigest && receipt.capture_id === capture &&
      ref.record_id === record && ref.revision_id === revision && ref.lifecycle_version === 1 &&
      ref.record_ref === expectedRef && text(receipt.receipt_ref) &&
      Array.isArray(receipt.attachments) && canonicalJson(receipt.attachments) === canonicalJson(query.attachments);
  } catch { return false; }
}

/** Fixed host-owned route and existing Access credentials; no event can supply
 * a URL, principal, selector or credential. This constructor does no I/O. */
export class PcpReceiver {
  readonly selectionDigest: string;
  private readonly selection: PcpSyncSelection;
  private readonly endpoint: string;
  private readonly clientId: string;
  private readonly clientSecret: string;

  constructor(
    selection: PcpSyncSelection,
    binding: { clientId: string; clientSecret: string },
    private readonly fetchFn: typeof fetch = fetch,
  ) {
    this.selection = freezeSelection(selection);
    this.selectionDigest = digest(canonicalJson(this.selection));
    if (!text(binding.clientId) || !text(binding.clientSecret) ||
        this.selection.policy.principal !== "service:" + binding.clientId) {
      throw new Error("Invalid fixed PCP Matrix receiver binding");
    }
    this.endpoint = this.selection.receiverUrl;
    this.clientId = binding.clientId;
    this.clientSecret = binding.clientSecret;
  }

  private async post(operation: "capture" | "reconcile" | "decision-scope", body: string, context: { room: string; mode: AcquisitionMode }): Promise<unknown> {
    if (Buffer.byteLength(body) > (operation === "capture" ? allJoinedSelection(this.selection) ? 24 * 1024 * 1024 : 2_000_000 : operation === "decision-scope" ? 512 * 1024 : 8192)) {
      throw new Error("Matrix PCP request exceeds receiver bound");
    }
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 15_000);
    try {
      const response = await this.fetchFn(this.endpoint, {
        method: "POST", redirect: "error", signal: controller.signal,
        headers: {
          "Content-Type": "application/json",
          "CF-Access-Client-Id": this.clientId, "CF-Access-Client-Secret": this.clientSecret,
          "x-ea-source": "matrix", "x-ea-account": this.selection.policy.account,
          "x-ea-room": context.room, "x-ea-operation": operation,
          ...(allJoinedSelection(this.selection) ? { "x-ea-acquisition-mode": context.mode } : {}),
        },
        body,
      });
      if (response.status !== 200) {
        (response.body as Readable | null)?.destroy();
        throw new Error("Matrix PCP receiver did not return an exact successful read");
      }
      if (operation === "capture") {
        // Even a complete capture response is followed by the separate exact
        // read-only proof. Never interpret a lost body as permission to replay.
        (response.body as Readable | null)?.destroy();
        return null;
      }
      if (!response.body) throw new Error("Matrix PCP receipt body unavailable");
      const chunks: Buffer[] = [];
      let size = 0;
      for await (const chunk of response.body) {
        const bytes = Buffer.from(chunk); size += bytes.length;
        if (size > 65_536) {
          (response.body as Readable).destroy(); throw new Error("Matrix PCP receipt exceeds bound");
        }
        chunks.push(bytes);
      }
      return JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(Buffer.concat(chunks)));
    } finally { clearTimeout(timeout); }
  }

  async decisionScope(snapshot: DecisionRoomsSnapshot): Promise<void> {
    if (!allJoinedSelection(this.selection)) return;
    const receipt = await this.post("decision-scope", JSON.stringify(snapshot), {room:"!decisions-snapshot",mode:"live"});
    if (!object(receipt) || receipt.status !== "accepted") throw new Error("Matrix Decisions scope was not retained");
  }

  capture(body: string): Promise<unknown> {
    return this.post("capture", body, publicationContext(JSON.parse(body), this.selection));
  }
  reconcile(query: string, context?: { room: string; mode: AcquisitionMode }): Promise<unknown> {
    if (this.selection.policy.schema === "ea-chat-source-v1") context = { room: this.selection.policy.room, mode: this.selection.policy.mode };
    if (!context) throw new Error("Matrix reconcile requires its frozen publication context");
    return this.post("reconcile", query, context);
  }

  /** Large originals use the shared content-addressed staging protocol. A
   * transfer acknowledgement is separate from capture of the logical event. */
  async uploadOriginal(context: MediaContext, locator: string, file: SpooledMedia, signal: AbortSignal,
    canContinue: () => boolean = () => true): Promise<MediaOriginal> {
    if (!allJoinedSelection(this.selection) || !text(context.itemId) || !text(context.room) || !text(locator) ||
        file.byte_size < 1 || file.byte_size > MAX_MEDIA_ORIGINAL_BYTES || file.chunks.length !== Math.ceil(file.byte_size / MEDIA_CHUNK_BYTES)) {
      throw new Error("Invalid Matrix original upload");
    }
    const item = canonicalJson([this.selection.policy.profile, context.room, context.itemId]);
    const scope = digest(canonicalJson(["ea-original-upload-v1", ["matrix", this.selection.policy.account, item, digest(locator)]]));
    const expected: MediaOriginal = { object_ref: "r2://ea-raw/sources/common/streamed-originals/v1/" + scope + "/" + file.sha256,
      sha256: file.sha256, byte_size: file.byte_size, media_type: file.media_type, source_locator: null };
    const base = { "CF-Access-Client-Id": this.clientId, "CF-Access-Client-Secret": this.clientSecret,
      "x-ea-source": "matrix", "x-ea-account": this.selection.policy.account, "x-ea-room": context.room,
      "x-ea-acquisition-mode": context.mode, "x-ea-operation": "original", "x-ea-item-id": context.itemId,
      "x-ea-media-id": digest(locator), "x-ea-original-sha256": file.sha256,
      "x-ea-original-size": String(file.byte_size), "x-ea-original-media-type": file.media_type };
    const request = async (operation: string, method = "GET", extra = {}, body?: string | Buffer): Promise<any> => {
      if (!canContinue()) throw new Error("Matrix media waits for live work");
      const deadline = boundedRequestSignal(signal, operation === "complete" ? 180_000 : 60_000);
      try {
        const response = await this.fetchFn(this.endpoint, { method, redirect: "error", signal: deadline.signal,
          headers: { ...base, "x-ea-original-operation": operation, ...extra }, body });
        if (response.status !== 200 || !response.body) { (response.body as Readable | null)?.destroy(); throw new Error("Matrix original response unavailable"); }
        const chunks: Buffer[] = []; let size = 0;
        for await (const chunk of response.body) {
          const bytes = Buffer.from(chunk); size += bytes.length;
          if (size > 32_768) { (response.body as Readable).destroy(); throw new Error("Matrix original response exceeds bound"); }
          chunks.push(bytes);
        }
        return JSON.parse(Buffer.concat(chunks).toString("utf8"));
      } finally { deadline.dispose(); }
    };
    const retained = (result: any) => result?.status === "retained" && canonicalJson(result.original) === canonicalJson(expected);
    let status = await request("status");
    if (!retained(status)) {
      if (status?.status !== "absent") throw new Error("Matrix original upload status unavailable");
      const handle = await open(file.path, constants.O_RDONLY | constants.O_NOFOLLOW);
      try {
        const stat = await handle.stat();
        if (!stat.isFile() || stat.uid !== process.getuid!() || stat.nlink !== 1 || stat.mode & 0o077 || stat.size !== file.byte_size) throw new Error("Private Matrix original changed");
        for (let index = 0; index < file.chunks.length; index++) {
          const sha256 = file.chunks[index], size = Math.min(MEDIA_CHUNK_BYTES, file.byte_size - index * MEDIA_CHUNK_BYTES);
          const headers = { "x-ea-chunk-index": String(index), "x-ea-chunk-sha256": sha256 };
          const matches = (part: any) => part?.status === "retained" && part.part === index && part.sha256 === sha256 && part.byte_size === size;
          let part = await request("chunk", "GET", headers);
          if (matches(part)) continue;
          if (part?.status !== "absent") throw new Error("Matrix original chunk status unavailable");
          const bytes = Buffer.alloc(size); let offset = 0;
          while (offset < size) {
            const read = await handle.read(bytes, offset, size - offset, index * MEDIA_CHUNK_BYTES + offset);
            if (!read.bytesRead) throw new Error("Matrix original spool incomplete"); offset += read.bytesRead;
          }
          if (digest(bytes) !== sha256) throw new Error("Matrix original spool digest differs");
          part = await request("chunk", "PUT", { ...headers, "Content-Type": "application/octet-stream", "Content-Length": String(size) }, bytes);
          if (!matches(part)) throw new Error("Matrix original chunk pending");
        }
      } finally { await handle.close(); }
      status = await request("complete", "POST", { "Content-Type": "application/json" }, JSON.stringify({ chunks: file.chunks }));
      if (!retained(status)) throw new Error("Matrix original finalization pending");
    }
    return { ...expected, source_locator: locator };
  }
}

/** One selected obligation per call. No timer, automatic replay or provider
 * acquisition is introduced. A healthy sibling is independently publishable. */
export async function publishPcpObservation(
  handoff: PcpSyncHandoff, id: string, receiver: PcpReceiver,
): Promise<"acknowledged" | "unresolved"> {
  if (receiver.selectionDigest !== handoff.selectionDigest) throw new Error("Matrix PCP publisher selection mismatch");
  const claimed = handoff.claim(id);
  if (claimed) {
    try { await receiver.capture(claimed.body); } catch { /* effect remains uncertain */ }
  }
  return reconcilePcpObservation(handoff, id, receiver);
}

export async function reconcilePcpObservation(
  handoff: PcpSyncHandoff, id: string, receiver: PcpReceiver,
): Promise<"acknowledged" | "unresolved"> {
  if (receiver.selectionDigest !== handoff.selectionDigest) throw new Error("Matrix PCP publisher selection mismatch");
  if (handoff.state(id) === "acknowledged") return "acknowledged";
  if (handoff.state(id) !== "uncertain") return "unresolved";
  const frozen = handoff.publication(id);
  if (!frozen) return "unresolved";
  try {
    const receipt = await receiver.reconcile(frozen.query, publicationContext(JSON.parse(frozen.body), handoff.selection));
    return handoff.acknowledge(id, receipt) ? "acknowledged" : "unresolved";
  } catch { return "unresolved"; }
}
