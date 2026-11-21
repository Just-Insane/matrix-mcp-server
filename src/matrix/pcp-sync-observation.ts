import { createHash } from "node:crypto";

type Json = null | boolean | number | string | Json[] | { [key: string]: Json };
type ObjectValue = { [key: string]: Json };
export const object = (value: unknown): value is ObjectValue =>
  value !== null && typeof value === "object" && !Array.isArray(value);
export const digest = (value: string | Uint8Array): string =>
  createHash("sha256").update(value).digest("hex");
export const canonicalJson = (value: unknown): string => JSON.stringify(
  Array.isArray(value) ? value.map(value => JSON.parse(canonicalJson(value)))
    : object(value) ? Object.fromEntries(Object.keys(value).sort().map(key =>
      [key, JSON.parse(canonicalJson(value[key]))])) : value,
);
const text = (value: unknown, max = 256): value is string =>
  typeof value === "string" && value.length > 0 && value.length <= max &&
  !/[\u0000-\u001f\u007f]/.test(value);

/** The receiver policy is supplied by the approved host binding, never a room
 * event, display name, caller header or bridge heuristic. No bridge is claimed. */
export type AcquisitionMode = "live" | "historical";
interface PcpPolicy {
  source: "matrix";
  account: string;
  profile: string;
  principal: string;
  policy_version: string;
  basis: string;
}
export interface PcpSyncSelection {
  homeserverUrl: string;
  userId: string;
  deviceId: string;
  receiverUrl: string;
  policy: PcpPolicy & ({
    schema: "ea-chat-source-v1";
    room: string;
    mode: AcquisitionMode;
  } | {
    schema: "ea-chat-source-v2";
    room_scope: "all_joined";
    mode: "live_and_historical";
  });
}

export const allJoinedSelection = (selection: PcpSyncSelection): boolean =>
  selection.policy.schema === "ea-chat-source-v2";
export const nativeRoomId = (room: unknown): room is string => text(room) && room.startsWith("!");
export function publicationContext(value: unknown, selection: PcpSyncSelection): { room: string; mode: AcquisitionMode } {
  if (!object(value) || !object(value.observation) || !nativeRoomId(value.observation.room_id)) {
    throw new Error("Invalid frozen Matrix publication context");
  }
  const room = value.observation.room_id;
  if (selection.policy.schema === "ea-chat-source-v1") {
    if (room !== selection.policy.room || value.acquisition !== undefined) throw new Error("Matrix publication scope differs");
    return { room, mode: selection.policy.mode };
  }
  if (!object(value.acquisition) || Object.keys(value.acquisition).join() !== "mode" ||
      !["live", "historical"].includes(String(value.acquisition.mode))) throw new Error("Invalid Matrix acquisition lane");
  return { room, mode: value.acquisition.mode as AcquisitionMode };
}

export function freezeSelection(input: PcpSyncSelection): PcpSyncSelection {
  const value = JSON.parse(JSON.stringify(input));
  const policy = value?.policy;
  const exact = (v: unknown, keys: string[]) => object(v) &&
    Object.keys(v).sort().join() === keys.sort().join();
  if (!exact(value, ["homeserverUrl", "userId", "deviceId", "receiverUrl", "policy"]) ||
      !(policy?.schema === "ea-chat-source-v1"
        ? exact(policy, ["schema", "source", "account", "profile", "room", "principal", "policy_version", "mode", "basis"]) &&
          ["live", "historical"].includes(policy.mode) && nativeRoomId(policy.room)
        : policy?.schema === "ea-chat-source-v2" &&
          exact(policy, ["schema", "source", "account", "profile", "room_scope", "principal", "policy_version", "mode", "basis"]) &&
          policy.room_scope === "all_joined" && policy.mode === "live_and_historical") ||
      policy.source !== "matrix" ||
      !["account", "profile", "principal", "policy_version", "basis"]
        .every(key => text(policy[key])) ||
      !policy.principal.startsWith("service:") ||
      !text(value.userId) || !value.userId.startsWith("@") || !text(value.deviceId)) {
    throw new Error("Invalid fixed Matrix PCP selection");
  }
  const home = new URL(value.homeserverUrl);
  if (home.protocol !== "https:" || home.username || home.password || home.search ||
      home.hash || home.pathname !== "/") throw new Error("Invalid selected Matrix homeserver");
  value.homeserverUrl = home.origin;
  const receiver = new URL(value.receiverUrl);
  if (receiver.protocol !== "https:" || receiver.username || receiver.password ||
      receiver.search || receiver.hash || receiver.pathname !== "/v1/source/matrix-discord") {
    throw new Error("Invalid fixed PCP Matrix receiver");
  }
  value.receiverUrl = receiver.href;
  Object.freeze(value.policy);
  return Object.freeze(value);
}

export interface SelectedObservation {
  roomId: string;
  mode: AcquisitionMode;
  eventId: string | null;
  wire: Buffer;
  wireSha256: string;
  section: string;
  stream: string;
  ordinal: number;
  publication: string | null;
  query: string | null;
  queryDigest: string | null;
  targetEventId: string | null;
  gaps: string[];
}
export interface SelectedSync {
  nextBatch: string;
  items: SelectedObservation[];
  coverage: { roomId: string | null; section: string; gaps: string[]; prevBatch: string | null; lastEventId: string | null }[];
}

/** Strict JSON span parsing retains the selected event's exact UTF-8 spelling.
 * SDK eventMapper adds room_id and mutates unsigned; neither is wire custody.
 * Duplicate keys and excessive nesting are rejected before a cursor can move. */
function parseSpans(bytes: Buffer): { value: Json; elements: WeakMap<object, [number, number][]>; source: string } {
  const source = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  const elements = new WeakMap<object, [number, number][]>();
  let at = 0;
  const ws = () => { while (/[ \t\r\n]/.test(source[at] ?? "x")) at++; };
  const string = (): string => {
    const start = at++;
    while (at < source.length) {
      if (source[at] === "\\") { at += 2; continue; }
      if (source[at++] === '"') {
        try { return JSON.parse(source.slice(start, at)); }
        catch { throw new Error("Invalid Matrix JSON string"); }
      }
    }
    throw new Error("Unterminated Matrix JSON string");
  };
  const value = (depth: number): Json => {
    if (depth > 64) throw new Error("Matrix JSON nesting exceeds retention bound");
    ws();
    if (source[at] === '"') return string();
    if (source[at] === "{" || source[at] === "[") {
      const array = source[at++] === "[";
      const out: Json[] | ObjectValue = array ? [] : Object.create(null);
      const entries: [number, number][] = [];
      const end = array ? "]" : "}";
      ws();
      if (source[at] !== end) while (true) {
        if (array) {
          const start = at;
          (out as Json[]).push(value(depth + 1));
          entries.push([start, at]);
        }
        else {
          if (source[at] !== '"') throw new Error("Invalid Matrix JSON key");
          const key = string(); ws();
          if (Object.hasOwn(out, key)) throw new Error("Ambiguous duplicate Matrix JSON key");
          if (source[at++] !== ":") throw new Error("Invalid Matrix JSON object");
          (out as ObjectValue)[key] = value(depth + 1);
        }
        ws();
        if (source[at] !== ",") break;
        at++; ws();
      }
      if (source[at++] !== end) throw new Error("Invalid Matrix JSON container");
      if (array) elements.set(out, entries);
      return out;
    }
    const match = /^(?:null|true|false|-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?)/.exec(source.slice(at));
    if (!match) throw new Error("Invalid Matrix JSON value");
    at += match[0].length;
    return JSON.parse(match[0]);
  };
  const result = value(0); ws();
  if (at !== source.length) throw new Error("Trailing Matrix JSON content");
  return { value: result, elements, source };
}

function prepareObservation(
  selection: PcpSyncSelection, raw: Json, wire: Buffer, section: string,
  stream: string, ordinal: number, observedAt: string, batchGaps: string[], roomId: string, mode: AcquisitionMode,
): SelectedObservation {
  const gaps = [...batchGaps, "upstream_history_unverified"];
  const result: SelectedObservation = {
    roomId, mode,
    eventId: object(raw) && text(raw.event_id) ? raw.event_id : null,
    wire, wireSha256: digest(wire), section, stream, ordinal,
    publication: null, query: null, queryDigest: null, targetEventId: null, gaps,
  };
  if (!object(raw) || !result.eventId || !result.eventId.startsWith("$")) {
    gaps.push("native_event_identity_unavailable"); return result;
  }
  if (raw.room_id !== undefined && raw.room_id !== roomId) {
    gaps.push("native_room_mismatch"); return result;
  }
  const content = object(raw.content) ? raw.content : {};
  const relation = object(content["m.relates_to"]) ? content["m.relates_to"] : {};
  const edit = relation.rel_type === "m.replace";
  const redaction = raw.type === "m.room.redaction";
  if (edit || redaction) {
    const target = edit ? relation.event_id : content.redacts ?? raw.redacts;
    result.targetEventId = text(target, 2048) ? target : null;
    gaps.push("lifecycle_contract_pending");
    return result;
  }
  if (object(raw.unsigned) && raw.unsigned.redacted_because !== undefined) {
    gaps.push("lifecycle_contract_pending"); return result;
  }
  if (!["m.room.message", "m.room.encrypted"].includes(String(raw.type)) || stream !== "timeline") {
    gaps.push("non_message_observation");
    if (!allJoinedSelection(selection)) return result;
  }
  const encrypted = raw.type === "m.room.encrypted";
  if (encrypted) gaps.push("decryption_unavailable", "attachment_inventory_unavailable");
  const info = object(content.info) ? content.info : {};
  const file = object(content.file) ? content.file : {};
  const thumbnail = object(info.thumbnail_file) ? info.thumbnail_file : {};
  const locators = [...new Set([content.url, file.url, info.thumbnail_url, thumbnail.url]
    .filter((value): value is string => text(value, 2048)))];
  if (locators.length) gaps.push("attachment_bytes_unavailable");
  // Adding the enclosing room to the raw envelope is an explicit transformation.
  const fidelity = raw.room_id === roomId ? "original" : "transformed";
  const providerRaw = fidelity === "original" ? raw : { ...raw, room_id: roomId };
  let time = typeof raw.origin_server_ts === "number" && Number.isSafeInteger(raw.origin_server_ts)
    && Math.abs(raw.origin_server_ts) <= 8_640_000_000_000_000
    ? new Date(raw.origin_server_ts).toISOString() : null;
  if (time && !/^\d{4}-/.test(time)) time = null;
  if (time === null) gaps.push("native_event_time_unavailable");
  const observation = {
    schema: "matrix-discord-observation/v1", source: "matrix", account: selection.policy.account,
    room_id: roomId, event_id: result.eventId,
    thread_id: ["m.thread", "io.element.thread"].includes(String(relation.rel_type)) &&
      text(relation.event_id, 2048) ? relation.event_id : null,
    observed_at: observedAt, event_time: time, raw: providerRaw,
    decryption: encrypted ? "missing" : "available", coverage: "partial",
    bridge: { status: "none", native_id: null, bridged_id: null },
    gaps, relation: { kind: "original", target_event_id: null },
    attachments: locators.map(locator => ({ locator, custody: "reference" })),
  };
  if (Buffer.byteLength(JSON.stringify(observation)) > 1_000_000) {
    gaps.push("observation_exceeds_receiver_bound"); return result;
  }
  const { observed_at: _clock, ...meaning } = observation;
  const query = {
    schema: "ea-chat-receipt-query-v1", event_id: result.eventId,
    native_sha256: digest(fidelity === "original" ? wire : JSON.stringify(providerRaw)),
    observation_digest: digest(canonicalJson(meaning)), fidelity, attachments: [],
  };
  result.publication = JSON.stringify({
    observation, ...(fidelity === "original" ? { provider_bytes: wire.toString("base64") } : {}),
    ...(allJoinedSelection(selection) ? { acquisition: { mode } } : {}),
  });
  if (Buffer.byteLength(result.publication) > 2_000_000) {
    result.publication = null;
    gaps.push("publication_exceeds_receiver_bound");
    return result;
  }
  result.query = JSON.stringify(query);
  result.queryDigest = digest(canonicalJson(query));
  return result;
}

export function selectSync(bytes: Buffer, selection: PcpSyncSelection, observedAt: string,
  previouslyJoined: ReadonlySet<string> = new Set()): SelectedSync {
  if (!/^\d{4}-\d{2}-\d{2}T.+(?:Z|[+-]\d{2}:\d{2})$/.test(observedAt) ||
      !Number.isFinite(Date.parse(observedAt))) throw new Error("Invalid Matrix observation clock");
  const { value, elements, source } = parseSpans(bytes);
  if (!object(value) || !text(value.next_batch, 16_384)) throw new Error("Matrix sync checkpoint missing");
  if (value.rooms !== undefined && !object(value.rooms)) throw new Error("Invalid Matrix rooms container");
  const result: SelectedSync = { nextBatch: value.next_batch, items: [], coverage: [] };
  const rooms = object(value.rooms) ? value.rooms : {};
  const sections = ["join", "leave", "invite", "knock"];
  for (const [section, group] of Object.entries(rooms)) {
    if (!sections.includes(section) && object(group) && (allJoinedSelection(selection) ||
        group[(selection.policy as { room: string }).room] !== undefined)) {
      throw new Error("Unsupported selected Matrix room section");
    }
  }
  for (const section of sections) {
    const group = rooms[section];
    if (group === undefined) continue;
    if (!object(group)) throw new Error("Invalid Matrix room group");
    const selectedRooms = selection.policy.schema === "ea-chat-source-v1" ? [selection.policy.room]
      : section === "join" ? Object.keys(group) : section === "leave"
        ? Object.keys(group).filter(room => previouslyJoined.has(room)) : [];
    for (const roomId of selectedRooms) {
      const room = group[roomId];
      if (room === undefined) continue;
      if (!nativeRoomId(roomId) || !object(room)) throw new Error("Invalid selected Matrix room");
      const timeline = object(room.timeline) ? room.timeline : {};
      const gaps = timeline.limited === true ? ["timeline_limited"] : [];
      const events = Array.isArray(timeline.events) ? timeline.events : [];
      const last = events.at(-1);
      result.coverage.push({
        roomId, section, gaps: [...gaps, "upstream_history_unverified"],
        prevBatch: text(timeline.prev_batch, 16_384) ? timeline.prev_batch : null,
        lastEventId: object(last) && text(last.event_id) ? last.event_id : null,
      });
      for (const stream of ["timeline", "state", "invite_state", "knock_state", "org.matrix.msc4222.state_after"]) {
        const container = room[stream];
        if (container === undefined) continue;
        if (!object(container) || !Array.isArray(container.events)) throw new Error("Unenumerable selected Matrix events");
        const events = container.events;
        events.forEach((raw, ordinal) => {
          const span = elements.get(events)?.[ordinal];
          if (!span) throw new Error("Selected Matrix event span unavailable");
          const wire = Buffer.from(source.slice(...span), "utf8");
          result.items.push(prepareObservation(selection, raw, wire, section, stream, ordinal, observedAt, gaps,
            roomId, selection.policy.schema === "ea-chat-source-v1" ? selection.policy.mode : "live"));
        });
      }
    }
  }
  if (!result.coverage.length) result.coverage.push({
    roomId: null, section: "unobserved", gaps: ["selected_room_not_observed", "upstream_history_unverified"], prevBatch: null, lastEventId: null,
  });
  return result;
}

/** Native /messages pages have an independent per-room continuation. Exact
 * event spellings are retained before that continuation can advance; an empty
 * page with an end token is not completion. No SDK timeline cursor is used. */
export function selectHistory(bytes: Buffer, selection: PcpSyncSelection, roomId: string,
  observedAt: string): { items: SelectedObservation[]; next: string | null } {
  if (!nativeRoomId(roomId) || !allJoinedSelection(selection)) throw new Error("Invalid Matrix history scope");
  if (!/^\d{4}-\d{2}-\d{2}T.+(?:Z|[+-]\d{2}:\d{2})$/.test(observedAt) ||
      !Number.isFinite(Date.parse(observedAt))) throw new Error("Invalid Matrix observation clock");
  const { value, elements, source } = parseSpans(bytes);
  if (!object(value) || !Array.isArray(value.chunk) ||
      value.end !== undefined && !text(value.end, 16_384)) throw new Error("Invalid Matrix history page");
  const items: SelectedObservation[] = [];
  for (const stream of ["chunk", "state"]) {
    const events = value[stream];
    if (events === undefined) continue;
    if (!Array.isArray(events)) throw new Error("Unenumerable Matrix history page");
    events.forEach((raw, ordinal) => {
      const span = elements.get(events)?.[ordinal];
      if (!span) throw new Error("Matrix history event span unavailable");
      items.push(prepareObservation(selection, raw, Buffer.from(source.slice(...span), "utf8"),
        "history", stream === "chunk" ? "timeline" : "state", ordinal, observedAt, [], roomId, "historical"));
    });
  }
  return { items, next: value.end as string | undefined ?? null };
}
