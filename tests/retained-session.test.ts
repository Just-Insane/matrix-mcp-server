import assert from "node:assert/strict";
import test from "node:test";
import { mkdtempSync, writeFileSync, readFileSync, readdirSync, rmSync, chmodSync, symlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { loadRetainedSession, retainedReadEnvironment, validateRetainedReadEnvironment,
  assertRetainedClientConfig } from "../src/matrix/retained-session.js";

function fixture(t: { after: (fn: () => void) => void }) {
  const dataDir = mkdtempSync(path.join(tmpdir(), "matrix-retained-test-"));
  t.after(() => rmSync(dataDir, { recursive: true, force: true }));
  const config = { dataDir, userId: "@alice:example.com", homeserverUrl: "https://matrix.example.com" };
  const login = { userId: config.userId, deviceId: "SAVED_DEVICE", accessToken: "dummy-retained-token" };
  writeFileSync(path.join(dataDir, "data-version"), "2");
  writeFileSync(path.join(dataDir, "login-state.json"), JSON.stringify(login), { mode: 0o600 });
  // A nonempty placeholder: tests never open or query a crypto database.
  writeFileSync(path.join(dataDir, "alice_example_com__matrix-sdk-crypto.sqlite"), "dummy existing crypto store");
  writeFileSync(path.join(dataDir, "ssss-recovery-key"), "must not be imported or deleted");
  return { config, login };
}

function snapshot(dir: string) {
  return Object.fromEntries(readdirSync(dir).map(name => [name, readFileSync(path.join(dir, name), "utf8")]));
}

test("retained launcher uses the existing session and neutralizes inherited bootstrap/dotenv inputs", t => {
  const { config, login } = fixture(t);
  const before = snapshot(config.dataDir);
  const env = retainedReadEnvironment(config, { PATH: "/bin", MATRIX_PASSWORD: "dummy-password",
    MATRIX_RECOVERY_KEY_FILE: "/dummy/recovery", MATRIX_RECOVERY_KEY: "dummy-key", MATRIX_SECURITY_KEY: "dummy-key",
    ENABLE_OAUTH: "true", ENABLE_TOKEN_EXCHANGE: "true", DOTENV_CONFIG_PATH: "/dummy/.env", DOTENV_KEY: "dummy-vault-key" });
  assert.equal(env.MATRIX_ACCESS_TOKEN, login.accessToken);
  assert.equal(env.MATRIX_DEVICE_ID, login.deviceId);
  assert.equal(env.MATRIX_DATA_DIR, config.dataDir);
  assert.equal(env.MATRIX_ALLOW_DEVICE_STORE_RECOVERY, "false");
  assert.equal(env.MATRIX_RESTORE_KEY_BACKUP, "false");
  assert.equal(env.MATRIX_PASSWORD, "");
  assert.equal(env.MATRIX_RECOVERY_KEY, "");
  assert.equal(env.MATRIX_SECURITY_KEY, "");
  assert.equal(env.MATRIX_RECOVERY_KEY_FILE, "");
  assert.equal(env.ENABLE_OAUTH, "false");
  assert.equal(env.ENABLE_TOKEN_EXCHANGE, "false");
  assert.equal(env.DOTENV_CONFIG_PATH, "/dev/null");
  assert.equal(env.DOTENV_KEY, "");
  assert.equal(env.PATH, "/bin");
  assert.deepEqual(validateRetainedReadEnvironment(env), { ...config, ...login });
  assert.deepEqual(snapshot(config.dataDir), before);
});

test("retained mode rejects missing, stale, and future version markers without modifying the store", t => {
  const { config } = fixture(t);
  for (const version of [null, "0", "1", "3", "invalid"]) {
    const marker = path.join(config.dataDir, "data-version");
    if (version === null) rmSync(marker); else writeFileSync(marker, version);
    const before = snapshot(config.dataDir);
    assert.throws(() => loadRetainedSession(config), /Retained Matrix read mode/);
    assert.deepEqual(snapshot(config.dataDir), before);
  }
});

test("retained mode requires a private saved login with the same user, device, and token", t => {
  const { config, login } = fixture(t);
  const file = path.join(config.dataDir, "login-state.json");
  for (const replacement of [ { ...login, userId: "@other:example.com" },
    { ...login, deviceId: "" }, { ...login, accessToken: "" }, { userId: login.userId } ]) {
    writeFileSync(file, JSON.stringify(replacement));
    assert.throws(() => loadRetainedSession(config), /saved user, device, and access token/);
  }
  writeFileSync(file, JSON.stringify(login));
  chmodSync(file, 0o644);
  assert.throws(() => loadRetainedSession(config), /mode 0600/);
  chmodSync(file, 0o600);
  rmSync(file);
  writeFileSync(path.join(config.dataDir, "other-login.json"), JSON.stringify(login), { mode: 0o600 });
  symlinkSync("other-login.json", file);
  assert.throws(() => loadRetainedSession(config), /regular file/);
});

test("retained mode refuses an absent crypto store instead of creating a fresh one", t => {
  const { config } = fixture(t);
  rmSync(path.join(config.dataDir, "alice_example_com__matrix-sdk-crypto.sqlite"));
  const before = snapshot(config.dataDir);
  assert.throws(() => loadRetainedSession(config), /existing user's primary crypto store/);
  assert.deepEqual(snapshot(config.dataDir), before);
});

test("a metadata-only database cannot substitute for the retained primary crypto store", t => {
  const { config } = fixture(t);
  rmSync(path.join(config.dataDir, "alice_example_com__matrix-sdk-crypto.sqlite"));
  writeFileSync(path.join(config.dataDir, "alice_example_com__matrix-sdk-crypto-meta.sqlite"), "dummy metadata");
  const before = snapshot(config.dataDir);
  assert.throws(() => loadRetainedSession(config), /primary crypto store is required/);
  assert.deepEqual(snapshot(config.dataDir), before);
});

test("source guards reject injected bootstrap inputs and alternate request credentials", t => {
  const { config } = fixture(t);
  const env = retainedReadEnvironment(config, {});
  for (const name of ["MATRIX_PASSWORD", "MATRIX_RECOVERY_KEY", "MATRIX_SECURITY_KEY", "MATRIX_RECOVERY_KEY_FILE", "ENABLE_OAUTH", "ENABLE_TOKEN_EXCHANGE"]) {
    assert.throws(() => validateRetainedReadEnvironment({ ...env, [name]: "true" }), /inputs are disabled/);
  }
  for (const name of ["MATRIX_ACCESS_TOKEN", "MATRIX_DEVICE_ID"]) {
    assert.throws(() => validateRetainedReadEnvironment({ ...env, [name]: "different" }), /match the saved session/);
  }
  const saved = loadRetainedSession(config);
  const client = { ...saved, enableOAuth: false, enableTokenExchange: false };
  assertRetainedClientConfig(saved, client);
  for (const changed of [ { userId: "@other:example.com" }, { deviceId: "OTHER" },
    { accessToken: "other-token" }, { homeserverUrl: "https://other.example.com" }, { enableOAuth: true } ]) {
    assert.throws(() => assertRetainedClientConfig(saved, { ...client, ...changed }), /saved client identity/);
  }
});

test("real module imports and MCP catalog leave fixture stores unchanged and never start the collector", t => {
  const { config } = fixture(t);
  const before = snapshot(config.dataDir);
  const script = `
    import { createServer } from './src/server.ts';
    import { startAutoSync, isAutoSyncRunning } from './src/matrix/autoSync.ts';
    import { getMessageQueue } from './src/matrix/messageQueue.ts';
    import { createMatrixClient } from './src/matrix/client.ts';
    import { Client } from '@modelcontextprotocol/sdk/client/index.js';
    import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js';
    await startAutoSync();
    let queueBlocked=false; try { getMessageQueue(); } catch { queueBlocked=true; }
    let alternateClientBlocked=false;
    try { await createMatrixClient({ homeserverUrl:process.env.MATRIX_HOMESERVER_URL, userId:process.env.MATRIX_USER_ID,
      accessToken:'different', deviceId:process.env.MATRIX_DEVICE_ID, enableOAuth:false, enableTokenExchange:false }); }
    catch { alternateClientBlocked=true; }
    const [left,right]=InMemoryTransport.createLinkedPair();
    const server=createServer(); const client=new Client({name:'fixture',version:'1'});
    await server.connect(right); await client.connect(left);
    const catalog=await client.listTools();
    await client.close(); await server.close();
    console.log(JSON.stringify({running:isAutoSyncRunning(),queueBlocked,alternateClientBlocked,
      tools:catalog.tools.map(tool=>tool.name)}));
  `;
  const output = execFileSync(process.execPath, ["--import", "tsx", "--input-type=module", "-e", script], {
    cwd: fileURLToPath(new URL("..", import.meta.url)),
    env: retainedReadEnvironment(config), encoding: "utf8", timeout: 15000,
  });
  const result = JSON.parse(output.trim());
  assert.equal(result.running, false);
  assert.equal(result.queueBlocked, true);
  assert.equal(result.alternateClientBlocked, true);
  assert.deepEqual(result.tools.sort(), ["get-messages-by-date", "get-room-event", "get-room-history", "get-room-info",
    "get-room-members", "get-room-messages", "identify-active-users", "list-active-rooms", "list-joined-rooms"].sort());
  assert.deepEqual(snapshot(config.dataDir), before);
});

test("module import fails before historical destructive migrations when a version marker is missing", t => {
  const { config } = fixture(t);
  const env = retainedReadEnvironment(config);
  rmSync(path.join(config.dataDir, "data-version"));
  const before = snapshot(config.dataDir);
  assert.throws(() => execFileSync(process.execPath, ["--import", "tsx", "--input-type=module", "-e",
    "await import('./src/matrix/client.ts')"], {
    cwd: fileURLToPath(new URL("..", import.meta.url)), env, timeout: 15000, stdio: "pipe",
  }));
  assert.deepEqual(snapshot(config.dataDir), before);
});
