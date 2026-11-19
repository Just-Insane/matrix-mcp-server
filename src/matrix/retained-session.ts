import { lstatSync, readFileSync } from "node:fs";
import path from "node:path";
import { CURRENT_DATA_VERSION } from "./migrations.js";

export interface RetainedSessionConfig {
  dataDir: string;
  userId: string;
  homeserverUrl: string;
}

export interface RetainedSession extends RetainedSessionConfig {
  deviceId: string;
  accessToken: string;
}

export const isRetainedReadMode = () => process.env.MATRIX_RETAINED_READ === "true";

export function getCryptoDatabasePrefix(userId: string): string {
  return userId.replace(/[^a-zA-Z0-9_]/g, "_").replace(/^_+/, "");
}

function fail(reason: string): never {
  throw new Error(`Retained Matrix read mode: ${reason}`);
}

/** Read existing session metadata only; never create, migrate, or open a database. */
export function loadRetainedSession(config: RetainedSessionConfig): RetainedSession {
  const { dataDir, userId, homeserverUrl } = config;
  if (!path.isAbsolute(dataDir) || !userId) fail("explicit data directory and user are required");
  let url: URL;
  try { url = new URL(homeserverUrl); } catch { fail("valid HTTPS homeserver is required"); }
  if (url.protocol !== "https:" || url.username || url.password || url.search || url.hash) {
    fail("valid HTTPS homeserver is required");
  }
  try {
    const directory = lstatSync(dataDir);
    if (!directory.isDirectory() || directory.uid !== process.getuid?.() || directory.mode & 0o022) {
      fail("existing data directory must be owned by this user and not writable by others");
    }
    const versionFile = path.join(dataDir, "data-version");
    if (!lstatSync(versionFile).isFile() || readFileSync(versionFile, "utf8").trim() !== String(CURRENT_DATA_VERSION)) {
      fail("data version must already be current; migrations are disabled");
    }
    const sessionFile = path.join(dataDir, "login-state.json");
    const file = lstatSync(sessionFile);
    if (!file.isFile() || file.uid !== process.getuid?.() || (file.mode & 0o777) !== 0o600 || file.size > 65536) {
      fail("saved login must be a user-owned regular file with mode 0600");
    }
    const saved = JSON.parse(readFileSync(sessionFile, "utf8"));
    if (saved.userId !== userId || typeof saved.deviceId !== "string" || !saved.deviceId.trim() ||
        typeof saved.accessToken !== "string" || !saved.accessToken.trim()) {
      fail("saved user, device, and access token must match the configured client");
    }
    const prefix = getCryptoDatabasePrefix(userId);
    // Rust SDK store `${prefix}::matrix-sdk-crypto`, sanitized by our SQLite
    // adapter. A metadata-only or similarly named file is not the primary store.
    const primaryStore = path.join(dataDir, `${prefix}__matrix-sdk-crypto.sqlite`);
    let cryptoFile;
    try { cryptoFile = lstatSync(primaryStore); } catch { fail("the existing user's primary crypto store is required"); }
    if (!cryptoFile.isFile() || cryptoFile.uid !== process.getuid?.() || cryptoFile.size === 0) {
      fail("the existing user's primary crypto store is required");
    }
    return { ...config, homeserverUrl: homeserverUrl.replace(/\/+$/, ""),
      deviceId: saved.deviceId, accessToken: saved.accessToken };
  } catch (error) {
    if (error instanceof Error && error.message.startsWith("Retained Matrix read mode:")) throw error;
    fail("saved session or current store metadata is missing or unreadable");
  }
}

const bootstrapVariables = [
  "MATRIX_PASSWORD", "MATRIX_RECOVERY_KEY", "MATRIX_SECURITY_KEY", "MATRIX_RECOVERY_KEY_FILE",
] as const;

export function retainedReadEnvironment(
  config: RetainedSessionConfig,
  inherited: NodeJS.ProcessEnv = process.env
): NodeJS.ProcessEnv {
  const saved = loadRetainedSession(config);
  return {
    ...inherited,
    ...Object.fromEntries(bootstrapVariables.map(name => [name, ""])),
    DOTENV_CONFIG_PATH: "/dev/null", DOTENV_CONFIG_OVERRIDE: "",
    DOTENV_KEY: "", DOTENV_CONFIG_DOTENV_KEY: "",
    ENABLE_OAUTH: "false", ENABLE_TOKEN_EXCHANGE: "false",
    MATRIX_RETAINED_READ: "true", MATRIX_ALLOW_DEVICE_STORE_RECOVERY: "false",
    MATRIX_RESTORE_KEY_BACKUP: "false",
    MATRIX_DATA_DIR: saved.dataDir, MATRIX_HOMESERVER_URL: saved.homeserverUrl,
    MATRIX_USER_ID: saved.userId, MATRIX_DEVICE_ID: saved.deviceId,
    MATRIX_ACCESS_TOKEN: saved.accessToken,
    // A dedicated read process has no hot-reload/action controller.
    MCP_CHILD: "1",
  };
}

/** Recheck after dotenv/imports, before any crypto database is opened. */
export function validateRetainedReadEnvironment(env = process.env): RetainedSession {
  if (bootstrapVariables.some(name => !!env[name]) || env.ENABLE_OAUTH === "true" || env.ENABLE_TOKEN_EXCHANGE === "true") {
    fail("password, OAuth, and recovery inputs are disabled");
  }
  const saved = loadRetainedSession({ dataDir: env.MATRIX_DATA_DIR ?? "",
    userId: env.MATRIX_USER_ID ?? "", homeserverUrl: env.MATRIX_HOMESERVER_URL ?? "" });
  if (env.MATRIX_ACCESS_TOKEN !== saved.accessToken || env.MATRIX_DEVICE_ID !== saved.deviceId) {
    fail("launch token and device must match the saved session");
  }
  return saved;
}

export function assertRetainedClientConfig(
  saved: RetainedSession,
  config: { userId: string; homeserverUrl: string; accessToken: string; deviceId?: string;
    enableOAuth: boolean; enableTokenExchange: boolean }
): void {
  if (config.userId !== saved.userId || config.homeserverUrl.replace(/\/+$/, "") !== saved.homeserverUrl ||
      config.accessToken !== saved.accessToken || config.deviceId !== saved.deviceId ||
      config.enableOAuth || config.enableTokenExchange) {
    fail("request must use the saved client identity and homeserver");
  }
}
