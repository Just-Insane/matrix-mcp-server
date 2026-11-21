import { closeSync, constants, fstatSync, openSync, readSync, realpathSync } from "node:fs";
import path from "node:path";
import { assertNativeSelection, freezePcpLegacyStart, type PcpLegacyStart } from "./pcp-sync-handoff.js";
import { digest, freezeSelection, type PcpSyncSelection } from "./pcp-sync-observation.js";

export interface PcpStartup {
  selection: PcpSyncSelection;
  initialBoundary?: PcpLegacyStart;
  receiverBinding?: { clientId: string; clientSecret: string };
}
const MAX_STARTUP_BYTES = 32 * 1024;

/** Read only the explicitly pinned host input. A file's existence, an event,
 * caller header or current cursor cannot select or activate this path. */
export function loadPcpStartup(env: NodeJS.ProcessEnv = process.env): PcpStartup | undefined {
  const filename = env.MATRIX_PCP_STARTUP_FILE;
  const expected = env.MATRIX_PCP_STARTUP_SHA256;
  if (filename === undefined && expected === undefined) return undefined;
  if (!filename || !path.isAbsolute(filename) || path.resolve(filename) !== filename ||
      !/^[a-f0-9]{64}$/.test(expected ?? "")) {
    throw new Error("Matrix PCP startup requires an explicit absolute file and exact digest pin");
  }
  if (env.MATRIX_RETAINED_READ !== "true" || env.MCP_CHILD !== "1" ||
      env.ENABLE_OAUTH === "true" || env.ENABLE_TOKEN_EXCHANGE === "true") {
    throw new Error("Matrix PCP startup requires the existing retained single-client route");
  }
  let bytes: Buffer;
  let fd: number | undefined;
  try {
    if (realpathSync(filename) !== filename) throw new Error();
    fd = openSync(filename, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
    const info = fstatSync(fd);
    if (!info.isFile() || info.uid !== process.getuid?.() || (info.mode & 0o777) !== 0o600 ||
        info.size < 1 || info.size > MAX_STARTUP_BYTES) throw new Error();
    const buffer = Buffer.alloc(MAX_STARTUP_BYTES + 1);
    let offset = 0;
    while (offset < buffer.length) {
      const count = readSync(fd, buffer, offset, buffer.length - offset, null);
      if (!count) break;
      offset += count;
    }
    if (offset > MAX_STARTUP_BYTES) throw new Error();
    bytes = buffer.subarray(0, offset);
  } catch {
    // Do not include private policy bytes, paths or filesystem error payloads.
    throw new Error("Matrix PCP startup private file is unavailable or unsafe");
  } finally {
    if (fd !== undefined) closeSync(fd);
  }
  if (digest(bytes) !== expected) throw new Error("Matrix PCP startup file differs from its exact digest pin");
  try {
    const value = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes));
    const publisher = value?.schema === "ea-matrix-pcp-startup-v2";
    const keys = ["schema", "selection", ...(Object.hasOwn(value ?? {}, "initialBoundary") ? ["initialBoundary"] : []),
      ...(publisher ? ["receiverBinding"] : [])];
    if (!value || typeof value !== "object" || Array.isArray(value) ||
        !["ea-matrix-pcp-startup-v1", "ea-matrix-pcp-startup-v2"].includes(value.schema) ||
        Object.keys(value).sort().join() !== keys.sort().join()) {
      throw new Error();
    }
    const selection = freezeSelection(value.selection);
    const initialBoundary = Object.hasOwn(value, "initialBoundary")
      ? freezePcpLegacyStart(value.initialBoundary, selection) : undefined;
    let receiverBinding: PcpStartup["receiverBinding"];
    if (publisher) {
      const binding = value.receiverBinding;
      if (selection.policy.schema !== "ea-chat-source-v2" || !binding || typeof binding !== "object" || Array.isArray(binding) ||
          Object.keys(binding).sort().join() !== "clientId,clientSecret" ||
          ![binding.clientId, binding.clientSecret].every(text => typeof text === "string" && text.length > 0 &&
            text.length <= 2048 && !/[\u0000-\u001f\u007f]/.test(text)) ||
          selection.policy.principal !== "service:" + binding.clientId) throw new Error();
      receiverBinding = Object.freeze({ clientId: binding.clientId, clientSecret: binding.clientSecret });
    }
    assertNativeSelection(selection, { homeserverUrl: env.MATRIX_HOMESERVER_URL ?? "",
      userId: env.MATRIX_USER_ID ?? null, deviceId: env.MATRIX_DEVICE_ID ?? null });
    return Object.freeze({ selection, ...(initialBoundary ? { initialBoundary } : {}), ...(receiverBinding ? { receiverBinding } : {}) });
  } catch {
    throw new Error("Matrix PCP startup fixed selection, native identity or initial boundary is invalid");
  }
}
