import * as sdk from "matrix-js-sdk";
import { MatrixClient, ClientEvent, EventTimeline } from "matrix-js-sdk";
import https from "https";
import fetch from "node-fetch";
import path from "path";
import { mkdirSync, existsSync, readFileSync, writeFileSync, readdirSync, renameSync } from "fs";
import { randomBytes } from "crypto";
import { exchangeToken, TokenExchangeConfig } from "../auth/tokenExchange.js";
import { getCachedClient, cacheClient, removeCachedClient } from "./clientCache.js";
import { installIDBAdapter } from "./idb-sqlite-adapter.js";
import { runMigrations } from "./migrations.js";
import { decodeRecoveryKey } from "./recovery-key.js";
import { getMessageQueue } from "./messageQueue.js";
import { resolveDeviceId } from "./device-id.js";
import { INITIAL_SYNC_TIMEOUT_MS, waitForUsableSync } from "./sync-ready.js";
import { boundedRequestSignal } from "./request-signal.js";

// Install SQLite-backed IndexedDB before any crypto init.
// Uses MATRIX_DATA_DIR env var, defaults to .data/ in cwd.
const DATA_DIR = process.env.MATRIX_DATA_DIR ?? path.join(process.cwd(), ".data");
mkdirSync(DATA_DIR, { recursive: true });
runMigrations(DATA_DIR);
installIDBAdapter(DATA_DIR);

type RecoveryKeySource = "env" | "file" | "local" | "generated";

interface RecoveryKeyState {
  key?: Uint8Array;
  source?: RecoveryKeySource;
  sourceName?: string;
}

function decodeRecoveryKeyMaterial(rawKey: string, source: string): Uint8Array {
  const trimmedKey = rawKey.trim();
  if (!trimmedKey) {
    throw new Error(`${source} is empty`);
  }

  // The local cache stores raw 32-byte key material as hex. User-facing Matrix
  // recovery keys use the Matrix cryptographic key representation.
  if (/^[0-9a-fA-F]{64}$/.test(trimmedKey)) {
    return new Uint8Array(Buffer.from(trimmedKey, "hex"));
  }

  return decodeRecoveryKey(trimmedKey);
}

function loadRecoveryKey(recoveryKeyFile: string): RecoveryKeyState {
  const envRecoveryKeyName = process.env.MATRIX_RECOVERY_KEY
    ? "MATRIX_RECOVERY_KEY"
    : process.env.MATRIX_SECURITY_KEY
      ? "MATRIX_SECURITY_KEY"
      : undefined;
  const envRecoveryKey = envRecoveryKeyName ? process.env[envRecoveryKeyName] : undefined;
  const recoveryKeyPath = process.env.MATRIX_RECOVERY_KEY_FILE;

  if (envRecoveryKey) {
    const key = decodeRecoveryKeyMaterial(envRecoveryKey, envRecoveryKeyName!);
    writeFileSync(recoveryKeyFile, Buffer.from(key).toString("hex"), { mode: 0o600 });
    console.error(`[E2EE] Using recovery key from ${envRecoveryKeyName}`);
    return { key, source: "env", sourceName: envRecoveryKeyName };
  }

  if (recoveryKeyPath) {
    const key = decodeRecoveryKeyMaterial(readFileSync(recoveryKeyPath, "utf-8"), "MATRIX_RECOVERY_KEY_FILE");
    writeFileSync(recoveryKeyFile, Buffer.from(key).toString("hex"), { mode: 0o600 });
    console.error(`[E2EE] Using recovery key from ${recoveryKeyPath}`);
    return { key, source: "file", sourceName: "MATRIX_RECOVERY_KEY_FILE" };
  }

  if (existsSync(recoveryKeyFile)) {
    return {
      key: decodeRecoveryKeyMaterial(readFileSync(recoveryKeyFile, "utf-8"), recoveryKeyFile),
      source: "local",
      sourceName: recoveryKeyFile,
    };
  }

  return {};
}

async function restoreKeyBackupFromSecretStorage(crypto: NonNullable<ReturnType<MatrixClient["getCrypto"]>>): Promise<any> {
  await crypto.loadSessionBackupPrivateKeyFromSecretStorage();
  console.error("[E2EE] Loaded session backup private key from secret storage");

  if (process.env.MATRIX_RESTORE_KEY_BACKUP === "false") {
    console.error("[E2EE] Skipping full key backup restore because MATRIX_RESTORE_KEY_BACKUP=false");
    return null;
  }

  console.error("[E2EE] Restoring room keys from server key backup; this may take a while");
  let lastProgressLogMs = 0;
  let lastProgressSuccesses = -1;
  let lastProgressStage: unknown;
  const result = await crypto.restoreKeyBackup({
    progressCallback: (progress) => {
      const now = Date.now();
      const successes = "successes" in progress && typeof progress.successes === "number"
        ? progress.successes
        : lastProgressSuccesses;
      const stageChanged = "stage" in progress && progress.stage !== lastProgressStage;
      const total = "total" in progress && typeof progress.total === "number" ? progress.total : undefined;
      const meaningfulIncrement = total
        ? successes - lastProgressSuccesses >= Math.max(500, Math.floor(total / 10))
        : false;
      const shouldLog =
        stageChanged ||
        (total !== undefined && successes === total) ||
        meaningfulIncrement ||
        now - lastProgressLogMs >= 10_000;
      if (shouldLog) {
        lastProgressLogMs = now;
        lastProgressSuccesses = successes;
        lastProgressStage = "stage" in progress ? progress.stage : lastProgressStage;
        console.log("[E2EE] Key backup restore progress: %j", progress);
      }
    },
  });
  console.error("[E2EE] Key backup restore complete: %j", result);
  return result;
}

/**
 * Configuration for Matrix client creation
 */
export interface MatrixClientConfig {
  homeserverUrl: string;
  userId: string;
  accessToken: string;
  deviceId?: string;
  enableOAuth: boolean;
  tokenExchangeConfig?: TokenExchangeConfig;
  enableTokenExchange: boolean;
  syncToken?: string;
}

const pendingClientCreations = new Map<string, Promise<MatrixClient>>();

function getClientCreationKey(userId: string, homeserverUrl: string): string {
  return `${userId}:${homeserverUrl}`;
}

function getCryptoDatabasePrefix(userId: string): string {
  return userId.replace(/[^a-zA-Z0-9_]/g, "_").replace(/^_+/, "");
}

function archiveCryptoStoreForUser(userId: string): void {
  const cryptoDbPrefix = getCryptoDatabasePrefix(userId);
  const matchingFiles = readdirSync(DATA_DIR).filter(
    (fileName) =>
      fileName.startsWith(`${cryptoDbPrefix}_`) ||
      fileName.startsWith(`${cryptoDbPrefix}-`)
  );
  if (matchingFiles.length === 0) return;

  const archiveDirectory = path.join(
    DATA_DIR,
    "stale-crypto",
    `${Date.now()}-${randomBytes(4).toString("hex")}`
  );
  mkdirSync(archiveDirectory, { recursive: true, mode: 0o700 });
  for (const fileName of matchingFiles) {
    renameSync(
      path.join(DATA_DIR, fileName),
      path.join(archiveDirectory, fileName)
    );
    console.error(`[E2EE] Archived stale crypto store file ${fileName}`);
  }
  console.error(`[E2EE] Stale crypto store preserved at ${archiveDirectory}`);
}

function restoreRoomPaginationTokens(client: MatrixClient): void {
  const queue = getMessageQueue();

  for (const room of client.getRooms()) {
    if (room.getMyMembership() !== "join") continue;
    if (room.oldState.paginationToken !== null) continue;

    const persistedToken = queue.getRoomPaginationToken(room.roomId);
    if (!persistedToken) continue;

    room.getLiveTimeline().setPaginationToken(persistedToken, EventTimeline.BACKWARDS);
  }
}

/**
 * Creates and initializes a Matrix client instance, using cache when possible
 *
 * @param config - Matrix client configuration
 * @returns Promise<MatrixClient> - Initialized Matrix client
 */
export async function createMatrixClient(
  config: MatrixClientConfig
): Promise<MatrixClient> {
  if (!config.homeserverUrl) {
    throw new Error("Homeserver URL is required to create a Matrix client.");
  }
  if (!config.userId) {
    throw new Error("User ID is required to create a Matrix client.");
  }

  const cachedClient = getCachedClient(config.userId, config.homeserverUrl);
  if (cachedClient) {
    return cachedClient;
  }

  const creationKey = getClientCreationKey(config.userId, config.homeserverUrl);
  const pendingClient = pendingClientCreations.get(creationKey);
  if (pendingClient) {
    console.error(`[Matrix] Reusing pending Matrix client creation for ${config.userId}`);
    return pendingClient;
  }

  const clientPromise = createMatrixClientUncached(config).finally(() => {
    pendingClientCreations.delete(creationKey);
  });
  pendingClientCreations.set(creationKey, clientPromise);
  return clientPromise;
}

async function createMatrixClientUncached(
  config: MatrixClientConfig,
  allowDeviceStoreRecovery = true
): Promise<MatrixClient> {
  const {
    homeserverUrl,
    userId,
    accessToken,
    deviceId: configuredDeviceId,
    enableOAuth,
    tokenExchangeConfig,
    enableTokenExchange,
    syncToken,
  } = config;

  if (!homeserverUrl) {
    throw new Error("Homeserver URL is required to create a Matrix client.");
  }
  if (!userId) {
    throw new Error("User ID is required to create a Matrix client.");
  }

  // Check for cached client first
  const cachedClient = getCachedClient(userId, homeserverUrl);
  if (cachedClient) {
    return cachedClient;
  }

  // No cached client, create a new one
  let matrixAccessToken: string;

  if (enableOAuth && enableTokenExchange) {
    if (!accessToken) {
      throw new Error("Access token is required for OAuth token exchange.");
    }
    if (!tokenExchangeConfig) {
      throw new Error(
        "Token exchange configuration is required for OAuth mode."
      );
    }
    matrixAccessToken = await exchangeToken(tokenExchangeConfig, accessToken);
  } else {
    // In non-OAuth mode, expect a direct Matrix access token
    matrixAccessToken = accessToken;
  }

  const FETCH_TIMEOUT_MS = 15_000;
  const SYNC_FETCH_TIMEOUT_MS = 65_000; // sync long-poll can wait 30s server-side
  const httpsAgent = new https.Agent({ rejectUnauthorized: false });

  const timedFetch = async (input: any, init?: any) => {
    // Use longer timeout for /sync long-poll requests
    const url = typeof input === "string" ? input : input?.url ?? "";
    const isSync = url.includes("/_matrix/client") && url.includes("/sync");
    const timeoutMs = isSync ? SYNC_FETCH_TIMEOUT_MS : FETCH_TIMEOUT_MS;
    const scope = boundedRequestSignal(init?.signal, timeoutMs);
    try {
      return await fetch(input, { ...(init || {}), agent: httpsAgent, signal: scope.signal as any }) as any;
    } catch (err: any) {
      if (isSync) console.error(`[Sync] /sync fetch failed: ${err.message}`);
      throw err;
    } finally {
      scope.dispose();
    }
  };

  // --- Authentication & Device ID ---
  // When MATRIX_PASSWORD is set (non-OAuth), use password login to get a proper device.
  // The shared-secret registration device ID ("shared_secret_registration") is a Dendrite
  // placeholder that breaks E2EE key distribution — other clients can't query device keys
  // for it. Password login creates a real device with proper key upload.
  const matrixPassword = process.env.MATRIX_PASSWORD;
  const loginStateFile = path.join(DATA_DIR, "login-state.json");
  let deviceId: string | undefined;
  let effectiveAccessToken = matrixAccessToken;

  if (matrixPassword && !enableOAuth) {
    // Try to reuse a previous password-login session (same device, no device accumulation)
    let loginState: { userId: string; deviceId: string; accessToken: string } | null = null;
    let previousLoginDeviceId: string | undefined;
    try {
      const saved = JSON.parse(readFileSync(loginStateFile, "utf-8"));
      if (saved.userId === userId && saved.deviceId && saved.accessToken) {
        loginState = saved;
        previousLoginDeviceId = saved.deviceId;
      }
    } catch { /* No saved state */ }

    if (loginState) {
      // Verify the saved token is still valid
      try {
        const whoamiRes = await timedFetch(`${homeserverUrl}/_matrix/client/v3/account/whoami`, {
          headers: { Authorization: `Bearer ${loginState.accessToken}` },
        });
        const whoami = await whoamiRes.json() as any;
        if (whoami.user_id === userId) {
          deviceId = loginState.deviceId;
          effectiveAccessToken = loginState.accessToken;
          console.error(`[Auth] Reusing saved login session (device: ${deviceId})`);
        } else {
          loginState = null; // Token valid but wrong user — re-login
        }
      } catch {
        loginState = null; // Token expired or invalid — re-login
        console.error("[Auth] Saved token invalid, will re-login with password");
      }
    }

    if (!loginState) {
      // Fresh password login — creates a real device with proper device ID
      console.error("[Auth] Logging in with password to get proper device ID");
      try {
        const loginRes = await timedFetch(`${homeserverUrl}/_matrix/client/v3/login`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            type: "m.login.password",
            identifier: { type: "m.id.user", user: userId },
            password: matrixPassword,
          }),
        });
        const loginData = await loginRes.json() as any;
        if (loginData.access_token && loginData.device_id) {
          deviceId = loginData.device_id;
          effectiveAccessToken = loginData.access_token;
          if (previousLoginDeviceId && previousLoginDeviceId !== loginData.device_id) {
            console.error(`[Auth] Device changed from ${previousLoginDeviceId} to ${loginData.device_id}; resetting stale crypto store`);
            archiveCryptoStoreForUser(userId);
          }
          // Persist so we reuse this device on restart
          writeFileSync(loginStateFile, JSON.stringify({
            userId,
            deviceId: loginData.device_id,
            accessToken: loginData.access_token,
          }), { mode: 0o600 });
          console.error(`[Auth] Password login successful (device: ${deviceId})`);
        } else {
          console.error("[Auth] Password login response missing token/device:", loginData);
          // Fall back to existing token
        }
      } catch (e: any) {
        console.error(`[Auth] Password login failed: ${e.message}, falling back to access token`);
      }
    }
  } else {
    // Access-token sessions need an explicit crypto device. Most homeservers
    // return it from whoami; MATRIX_DEVICE_ID covers homeservers/tokens that do not.
    deviceId = await resolveDeviceId({
      configuredDeviceId,
      homeserverUrl,
      userId,
      accessToken: matrixAccessToken,
      fetchFn: timedFetch,
    });
  }

  // Load SSSS recovery key material from env/file/local cache. This is needed
  // by getSecretStorageKey when the SDK unlocks 4S and key backup.
  const recoveryKeyFile = path.join(DATA_DIR, "ssss-recovery-key");
  const recoveryKeyState = loadRecoveryKey(recoveryKeyFile);
  let cachedRecoveryKey = recoveryKeyState.key;
  let recoveryKeySource = recoveryKeyState.source;
  const recoveryKeySourceName = recoveryKeyState.sourceName;
  const hasUserProvidedRecoveryKey = recoveryKeySource === "env" || recoveryKeySource === "file";
  let markInitialSyncReady: (() => void) | undefined;
  let markInitialSyncFailed: ((error: Error) => void) | undefined;
  const initialSyncReady = matrixPassword || hasUserProvidedRecoveryKey
    ? new Promise<void>((resolve, reject) => {
        markInitialSyncReady = resolve;
        markInitialSyncFailed = reject;
      })
    : Promise.resolve();

  const createSdkClient = () =>
    sdk.createClient({
      baseUrl: homeserverUrl,
      userId,
      ...(deviceId ? { deviceId } : {}),
      fetchFn: timedFetch,
      cryptoCallbacks: {
        // Supplies the SSSS decryption key when the SDK needs to read/write secrets.
        getSecretStorageKey: async ({ keys }) => {
          if (!cachedRecoveryKey) return null;
          const keyId = Object.keys(keys)[0];
          if (!keyId) return null;
          return [keyId, cachedRecoveryKey];
        },
        // Called after bootstrapSecretStorage creates a new key — cache it immediately.
        cacheSecretStorageKey: (_keyId, _keyInfo, key) => {
          cachedRecoveryKey = key;
        },
      },
    });

  const client = createSdkClient();

  try {
    if (enableOAuth && effectiveAccessToken && enableTokenExchange) {
      // OAuth mode: use token exchange result to login
      const matrixLoginResponse = await client.loginRequest({
        type: "org.matrix.login.jwt",
        token: effectiveAccessToken,
      });
      client.setAccessToken(matrixLoginResponse.access_token);
    } else if (effectiveAccessToken) {
      // Non-OAuth mode: use access token (from password login or env var)
      client.setAccessToken(effectiveAccessToken);
    } else {
      throw new Error("No valid access token available for Matrix client.");
    }

    // Enable E2EE with persistent SQLite-backed crypto store (always-on).
    // Use userId as crypto DB prefix so each user gets their own SQLite file.
    const cryptoDbPrefix = getCryptoDatabasePrefix(userId);
    await client.initRustCrypto({ useIndexedDB: true, cryptoDatabasePrefix: cryptoDbPrefix });
    console.error(`[E2EE] Crypto initialised. Device ID: ${client.getDeviceId()}`);

    // Phase 2: restore SSSS/key backup when a recovery key is supplied, or
    // bootstrap/manage cross-signing when MATRIX_PASSWORD is available.
    // IMPORTANT: Check cross-signing status BEFORE bootstrapping. If the user already
    // has cross-signing (e.g., from Element), creating new keys would reset their
    // identity and break trust with all previously verified devices.
    if (matrixPassword || hasUserProvidedRecoveryKey) {
      // Phase 2 runs in the background — don't block client creation.
      // E2EE will become available once bootstrap completes.
      (async () => {
      try {
        // Restoring thousands of backed-up room keys can monopolize the Rust
        // crypto worker. Let the initial /sync reach PREPARED first so client
        // creation and MCP reads do not time out while recovery runs.
        await initialSyncReady;
        const crypto = client.getCrypto();
        if (crypto) {
          // Load or generate a recovery key for SSSS.
          // Stored in DATA_DIR/ssss-recovery-key so it survives server restarts.
          let recoveryKeyBytes: Uint8Array;
          if (cachedRecoveryKey) {
            recoveryKeyBytes = cachedRecoveryKey;
            console.error("[E2EE] Using existing SSSS recovery key");
          } else {
            recoveryKeyBytes = new Uint8Array(randomBytes(32));
            writeFileSync(recoveryKeyFile, Buffer.from(recoveryKeyBytes).toString("hex"), { mode: 0o600 });
            cachedRecoveryKey = recoveryKeyBytes;
            recoveryKeySource = "generated";
            console.error("[E2EE] Generated new SSSS recovery key — saved to", recoveryKeyFile);
          }

          if (hasUserProvidedRecoveryKey) {
            let crossSigningBootstrapError: string | undefined;
            let crossSignDeviceError: string | undefined;
            const myDeviceId = client.getDeviceId();
            try {
              console.error("[E2EE] Restoring cross-signing keys from secret storage");
              await crypto.bootstrapCrossSigning({});
            } catch (e: any) {
              crossSigningBootstrapError = e.message;
              console.warn("[E2EE] Cross-signing restore failed:", e.message);
            }

            if (myDeviceId) {
              try {
                const devStatus = await crypto.getDeviceVerificationStatus(userId, myDeviceId);
                const crossSigningStatus = await crypto.getCrossSigningStatus();
                const hasCrossSigningPrivateKeys =
                  crossSigningStatus.privateKeysCachedLocally.masterKey &&
                  crossSigningStatus.privateKeysCachedLocally.selfSigningKey &&
                  crossSigningStatus.privateKeysCachedLocally.userSigningKey;
                if (devStatus && !devStatus.crossSigningVerified && hasCrossSigningPrivateKeys) {
                  console.error("[E2EE] Device not cross-signed after restore, signing now");
                  await crypto.crossSignDevice(myDeviceId);
                }
              } catch (e: any) {
                crossSignDeviceError = e.message;
                console.warn("[E2EE] Cross-signing current device failed:", e.message);
              }
            }

            let keyBackupRestoreResult: any = null;
            let keyBackupRestoreError: string | undefined;
            try {
              await crypto.checkKeyBackupAndEnable();
              keyBackupRestoreResult = await restoreKeyBackupFromSecretStorage(crypto);
            } catch (e: any) {
              keyBackupRestoreError = e.message;
              console.warn("[E2EE] Key backup restore failed:", e.message);
            }

            const diagPath = path.join(DATA_DIR, "e2ee-diagnostic.json");
            const myDiagDeviceId = client.getDeviceId();
            let diagDevStatus: any = null;
            if (myDiagDeviceId) {
              try {
                diagDevStatus = await crypto.getDeviceVerificationStatus(userId, myDiagDeviceId);
              } catch (e: any) {
                diagDevStatus = { error: e.message };
              }
            }
            writeFileSync(diagPath, JSON.stringify({
              timestamp: new Date().toISOString(),
              userId,
              deviceId: myDiagDeviceId,
              recoveryKeySource,
              recoveryKeySourceName,
              deviceVerificationStatus: diagDevStatus,
              crossSigningStatus: await crypto.getCrossSigningStatus(),
              crossSigningBootstrapError,
              crossSignDeviceError,
              keyBackupRestoreResult,
              keyBackupRestoreError,
            }, null, 2));
            return;
          }

          const crossSigningStatus = await crypto.getCrossSigningStatus();
          if (crossSigningStatus.privateKeysCachedLocally.masterKey) {
            // Private keys already cached locally from persistent crypto store.
            // No bootstrap needed — this device already has its identity.
            console.error("[E2EE] Cross-signing private keys cached locally, skipping bootstrap");
          } else if (crossSigningStatus.publicKeysOnDevice) {
            // Public keys exist (fetched from server) but private keys not available locally.
            // The user already has cross-signing from another device (e.g., Element).
            // Try to restore private keys from SSSS — do NOT create new ones.
            console.error("[E2EE] Cross-signing exists but private keys not local. Restoring from SSSS...");
            let restored = false;
            try {
              await crypto.bootstrapSecretStorage({
                createSecretStorageKey: async () => ({
                  keyInfo: {},
                  privateKey: recoveryKeyBytes,
                }),
              });
              await crypto.bootstrapCrossSigning({});
              const afterRestore = await crypto.getCrossSigningStatus();
              restored = afterRestore.privateKeysCachedLocally.masterKey;
            } catch (e: any) {
              console.warn("[E2EE] SSSS restore failed:", e.message);
            }
            if (!restored) {
              // SSSS restore didn't work (e.g., recovery key mismatch after migration,
              // or SSSS contains stale keys from old broken bootstrap). Delete stale SSSS
              // account data from server so bootstrapSecretStorage creates fresh without
              // trying to migrate old (undecryptable) secrets.
              console.error("[E2EE] SSSS restore failed. Clearing stale SSSS data then creating fresh.");
              try {
                // Clear the default key pointer and any secret storage keys
                const accountData = client.store.accountData;
                const ssssKeys = Object.keys(accountData || {}).filter(k =>
                  k.startsWith("m.secret_storage.key.") || k === "m.secret_storage.default_key"
                );
                for (const key of ssssKeys) {
                  await (client as any).setAccountData(key, {});
                  console.error(`[E2EE] Cleared stale account data: ${key}`);
                }
                // Also clear cross-signing secrets from SSSS
                for (const secret of ["m.cross_signing.master", "m.cross_signing.self_signing", "m.cross_signing.user_signing"]) {
                  try { await (client as any).setAccountData(secret, {}); } catch (_) {}
                }
              } catch (e: any) {
                console.warn("[E2EE] Failed to clear stale SSSS data:", e.message);
              }
              // Now create fresh cross-signing + SSSS from scratch
              await crypto.bootstrapCrossSigning({
                authUploadDeviceSigningKeys: async (makeRequest) => {
                  await makeRequest({
                    type: "m.login.password",
                    identifier: { type: "m.id.user", user: userId },
                    password: matrixPassword,
                  });
                },
              });
            }
          } else {
            // No cross-signing at all — safe to create new keys for this user.
            console.error("[E2EE] No existing cross-signing, creating new keys");
            await crypto.bootstrapCrossSigning({
              authUploadDeviceSigningKeys: async (makeRequest) => {
                await makeRequest({
                  type: "m.login.password",
                  identifier: { type: "m.id.user", user: userId },
                  password: matrixPassword,
                });
              },
            });
            await crypto.bootstrapSecretStorage({
              createSecretStorageKey: async () => ({
                keyInfo: {},
                privateKey: recoveryKeyBytes,
              }),
            });
          }

          // Verify the local device is cross-signed. bootstrapCrossSigning only
          // signs the device when CREATING new keys — SSSS restore skips this step.
          const myDeviceId = client.getDeviceId();
          if (myDeviceId) {
            const devStatus = await crypto.getDeviceVerificationStatus(userId, myDeviceId);
            if (devStatus && !devStatus.crossSigningVerified) {
              console.error("[E2EE] Device not cross-signed after bootstrap, signing now...");
              // Re-run bootstrap with auth to force device signing
              await crypto.bootstrapCrossSigning({
                authUploadDeviceSigningKeys: async (makeRequest) => {
                  await makeRequest({
                    type: "m.login.password",
                    identifier: { type: "m.id.user", user: userId },
                    password: matrixPassword,
                  });
                },
              });
              const afterSign = await crypto.getDeviceVerificationStatus(userId, myDeviceId);
              console.error("[E2EE] Device cross-signed after fix: %s", afterSign?.crossSigningVerified);
            } else {
              console.error("[E2EE] Device already cross-signed: %s", devStatus?.crossSigningVerified);
            }
          }

          await crypto.checkKeyBackupAndEnable();
          let keyBackupRestoreResult: any = null;
          let keyBackupRestoreError: string | undefined;
          if (cachedRecoveryKey) {
            try {
              keyBackupRestoreResult = await restoreKeyBackupFromSecretStorage(crypto);
            } catch (e: any) {
              keyBackupRestoreError = e.message;
              console.warn("[E2EE] Key backup restore failed:", e.message);
            }
          }
          const finalCrossSigningStatus = await crypto.getCrossSigningStatus();
          console.error("[E2EE] Phase 2 complete: cross-signing status: %j", finalCrossSigningStatus);
          // Write diagnostic file so we can check status without seeing stderr
          const diagPath = path.join(DATA_DIR, "e2ee-diagnostic.json");
          const myDiagDeviceId = client.getDeviceId();
          let diagDevStatus: any = null;
          if (myDiagDeviceId) {
            try {
              diagDevStatus = await crypto.getDeviceVerificationStatus(userId, myDiagDeviceId);
            } catch (e: any) {
              diagDevStatus = { error: e.message };
            }
          }
          writeFileSync(diagPath, JSON.stringify({
            timestamp: new Date().toISOString(),
            userId,
            deviceId: myDiagDeviceId,
            recoveryKeySource,
            recoveryKeySourceName,
            crossSigningStatus: finalCrossSigningStatus,
            deviceVerificationStatus: diagDevStatus,
            keyBackupRestoreResult,
            keyBackupRestoreError,
          }, null, 2));
        } // if (crypto)
      } catch (e: any) {
        console.warn("[E2EE] Phase 2 bootstrap failed (non-fatal):", e.message);
        const diagPath = path.join(DATA_DIR, "e2ee-diagnostic.json");
        writeFileSync(diagPath, JSON.stringify({
          timestamp: new Date().toISOString(),
          phase2Error: e.message,
          stack: e.stack?.split("\n").slice(0, 5),
        }, null, 2));
      }
      })();
    }

    // Resume from a persisted sync token so /sync starts from exactly where we left off.
    if (syncToken) {
      client.store.setSyncToken(syncToken);
      console.error(`[Sync] Resuming from stored sync token`);
    }

    // pollTimeout: server-side /sync long-poll timeout. Default is 30s, but reverse
    // proxies often have short idle timeouts that race with it. 10s is conservative
    // but ensures the /sync response arrives before any proxy kills the connection.
    await client.startClient({ initialSyncLimit: 20, pollTimeout: 10_000 });

    // Wait until sync is usable. startClient() can emit PREPARED before its
    // promise settles, so subscribe and re-check the current state atomically.
    await waitForUsableSync(
      () => client.getSyncState(),
      (listener) => {
        client.on(ClientEvent.Sync, listener);
        return () => client.removeListener(ClientEvent.Sync, listener);
      },
      INITIAL_SYNC_TIMEOUT_MS
    );
    markInitialSyncReady?.();

    // Set presence to online so other users can see the bot is active.
    // The homeserver automatically marks offline when /sync stops (e.g., laptop closed).
    try {
      await client.setPresence({ presence: "online" });
    } catch (_) {
      // Presence may not be supported by all homeservers (e.g., Dendrite)
    }

    restoreRoomPaginationTokens(client);

    // Cache the successfully created and synced client
    cacheClient(client, userId, homeserverUrl);
    
    return client;
  } catch (error) {
    markInitialSyncFailed?.(
      error instanceof Error ? error : new Error(String(error))
    );
    // If client creation failed, make sure to stop the client and don't cache it
    try {
      client.stopClient();
    } catch (stopError) {
      console.warn("Error stopping failed client:", stopError);
    }
    const message = error instanceof Error ? error.message : String(error);
    const isDeviceStoreMismatch =
      message.includes("account in the store doesn't match the account in the constructor");
    if (
      allowDeviceStoreRecovery &&
      isDeviceStoreMismatch &&
      !enableOAuth &&
      effectiveAccessToken &&
      deviceId
    ) {
      // The Rust SDK can defer the account/store identity check until
      // startClient() begins syncing, so recover at the outer client-start
      // boundary rather than only around initRustCrypto(). Retry once.
      console.error(
        `[E2EE] Matrix device changed to ${deviceId}; archiving incompatible crypto state and retrying`
      );
      archiveCryptoStoreForUser(userId);
      return createMatrixClientUncached(config, false);
    }
    throw error;
  }
}

/**
 * Remove a client from cache and stop it (for error recovery)
 *
 * @param userId - Matrix user ID  
 * @param homeserverUrl - Matrix homeserver URL
 */
export function removeClientFromCache(userId: string, homeserverUrl: string): void {
  pendingClientCreations.delete(getClientCreationKey(userId, homeserverUrl));
  removeCachedClient(userId, homeserverUrl);
}
