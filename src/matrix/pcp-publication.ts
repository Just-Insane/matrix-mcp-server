import fetch from "node-fetch";
import type { Readable } from "node:stream";
import type { PcpSyncHandoff, FrozenPublication } from "./pcp-sync-handoff.js";
import { canonicalJson, digest, freezeSelection, object, publicationContext, allJoinedSelection, type PcpSyncSelection, type AcquisitionMode } from "./pcp-sync-observation.js";

const text = (value: unknown): value is string =>
  typeof value === "string" && value.length > 0 && value.length <= 2048 && !/[\u0000-\u001f\u007f]/.test(value);

export function publicationQuery(body: string, selection: PcpSyncSelection): Record<string, unknown> {
  const value = JSON.parse(body), observation = value?.observation;
  const context = publicationContext(value, selection);
  if (!object(observation) || observation.schema !== "matrix-discord-observation/v1" ||
      observation.source !== "matrix" || observation.account !== selection.policy.account ||
      observation.room_id !== context.room || !text(observation.event_id) ||
      !object(observation.raw) || observation.raw.event_id !== observation.event_id ||
      observation.raw.room_id !== context.room || !allJoinedSelection(selection) && value.attachments !== undefined) {
    throw new Error("Frozen Matrix publication differs from selected source");
  }
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
      if (!object(attachment) || Object.keys(attachment).sort().join() !== "base64,locator,sha256" ||
          !text(attachment.locator) || typeof attachment.base64 !== "string" || !/^[a-f0-9]{64}$/.test(String(attachment.sha256)) ||
          !Array.isArray(observation.attachments) || !observation.attachments.some(item => object(item) && item.locator === attachment.locator)) {
        throw new Error("Invalid frozen Matrix attachment");
      }
      const bytes = Buffer.from(attachment.base64, "base64"); attachmentBytes += bytes.length;
      if (bytes.toString("base64") !== attachment.base64 || digest(bytes) !== attachment.sha256 ||
          attachmentBytes > 16 * 1024 * 1024) throw new Error("Matrix attachment bytes differ or exceed bound");
      attachments.push({ locator_sha256: digest(attachment.locator), sha256: String(attachment.sha256) });
    }
    if (new Set(attachments.map(item => item.locator_sha256)).size !== attachments.length) throw new Error("Duplicate Matrix attachment");
    attachments.sort((left, right) => left.locator_sha256 < right.locator_sha256 ? -1 : 1);
  }
  return {
    schema: "ea-chat-receipt-query-v1", event_id: observation.event_id,
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
    const item = canonicalJson([selection.policy.profile, publicationContext(JSON.parse(frozen.body), selection).room, query.event_id]);
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

  private async post(operation: "capture" | "reconcile", body: string, context: { room: string; mode: AcquisitionMode }): Promise<unknown> {
    if (Buffer.byteLength(body) > (operation === "capture" ? allJoinedSelection(this.selection) ? 24 * 1024 * 1024 : 2_000_000 : 8192)) {
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

  capture(body: string): Promise<unknown> {
    return this.post("capture", body, publicationContext(JSON.parse(body), this.selection));
  }
  reconcile(query: string, context?: { room: string; mode: AcquisitionMode }): Promise<unknown> {
    if (this.selection.policy.schema === "ea-chat-source-v1") context = { room: this.selection.policy.room, mode: this.selection.policy.mode };
    if (!context) throw new Error("Matrix reconcile requires its frozen publication context");
    return this.post("reconcile", query, context);
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
