import assert from "node:assert/strict";
import test, { type TestContext } from "node:test";
import { build } from "esbuild";
import { chmodSync, existsSync, mkdtempSync, mkdirSync, readFileSync, readdirSync,
  rmSync, symlinkSync, writeFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { spawnSync } from "node:child_process";
import { preparePcpLegacyStart } from "../src/matrix/pcp-sync-handoff.js";
import { digest, type PcpSyncSelection } from "../src/matrix/pcp-sync-observation.js";
import { getCryptoDatabasePrefix } from "../src/matrix/retained-session.js";
import { CURRENT_DATA_VERSION } from "../src/matrix/migrations.js";

const selection: PcpSyncSelection = {
  homeserverUrl: "https://matrix.example.com", userId: "@owner:example.com", deviceId: "FIXTURE",
  receiverUrl: "https://pcp.example.com/v1/source/matrix-discord",
  policy: { schema: "ea-chat-source-v1", source: "matrix", account: "@owner:example.com",
    room: "!selected:example.com", profile: "personal", principal: "service:fixture",
    policy_version: "fixture-v1", mode: "live", basis: "explicit-selection" },
};
const initialBoundary = preparePcpLegacyStart(selection, "fabricated-legacy", "fixture:approved-future-start");
const record = { schema: "ea-matrix-pcp-startup-v1", selection, initialBoundary };
const envKey = (name: string) => name.startsWith("MATRIX_") ||
  ["ENABLE_OAUTH", "ENABLE_TOKEN_EXCHANGE", "MCP_CHILD"].includes(name);

function fixture(t: TestContext) {
  const root = mkdtempSync(path.join(process.cwd(), ".pcp-startup-fixture-"));
  const file = path.join(root, "pcp.json"), state = path.join(root, "state");
  mkdirSync(state, { mode: 0o700 });
  writeFileSync(file, JSON.stringify(record), { mode: 0o600 });
  const previous = Object.fromEntries(Object.entries(process.env).filter(([key]) => envKey(key)));
  for (const key of Object.keys(process.env)) if (envKey(key)) delete process.env[key];
  Object.assign(process.env, { MATRIX_RETAINED_READ: "true", MCP_CHILD: "1",
    MATRIX_HOMESERVER_URL: selection.homeserverUrl, MATRIX_USER_ID: selection.userId,
    MATRIX_DEVICE_ID: selection.deviceId, MATRIX_ACCESS_TOKEN: "fabricated", MATRIX_DATA_DIR: state,
    MATRIX_PCP_STARTUP_FILE: file, MATRIX_PCP_STARTUP_SHA256: digest(readFileSync(file)) });
  t.after(() => {
    for (const key of Object.keys(process.env)) if (envKey(key)) delete process.env[key];
    Object.assign(process.env, previous); rmSync(root, { recursive: true, force: true });
  });
  return { root, file, state,
    write(value: unknown) { writeFileSync(file, JSON.stringify(value)); process.env.MATRIX_PCP_STARTUP_SHA256 = digest(readFileSync(file)); } };
}

async function configuredHelper(root: string) {
  // Exercise the real configured-client helper and loader, substituting only
  // the existing factory boundary. No Matrix request or database is opened.
  const source = fileURLToPath(new URL("../src/utils/server-helpers.ts", import.meta.url));
  const outfile = path.join(root, "configured-helper.mjs");
  await build({ stdin: { contents: `export {createConfiguredMatrixClient} from ${JSON.stringify(source)};
    export {calls,client} from 'fixture-client';`, resolveDir: process.cwd(), loader: "ts" },
    outfile, bundle: true, platform: "node", format: "esm", target: "node22", packages: "external",
    plugins: [{ name: "observe-existing-client-boundary", setup(builder) {
      builder.onResolve({ filter: /^fixture-client$|^\.\.\/matrix\/client\.js$/ }, args =>
        args.path === "fixture-client" || args.importer.endsWith("/src/utils/server-helpers.ts")
          ? { path: "client", namespace: "fixture-client" } : null);
      builder.onLoad({ filter: /.*/, namespace: "fixture-client" }, () => ({ loader: "js",
        contents: "export const calls=[]; export const client={fixture:true}; export async function createMatrixClient(config){calls.push(config);return client}" }));
    } }] });
  return import(pathToFileURL(outfile).href);
}

test("the configured helper supplies the exact pinned selection and decision before its first client", async t => {
  const f = fixture(t), helper = await configuredHelper(f.root);
  assert.equal(await helper.createConfiguredMatrixClient(selection.homeserverUrl, selection.userId, "fabricated", "fabricated-legacy"), helper.client);
  assert.equal(helper.calls.length, 1);
  const config = helper.calls[0];
  assert.deepEqual(config.pcpSync, selection);
  assert.deepEqual(config.pcpSyncStart, initialBoundary);
  assert.equal(config.syncToken, "fabricated-legacy");
  assert.ok(Object.isFrozen(config.pcpSync)); assert.ok(Object.isFrozen(config.pcpSync.policy));
  assert.ok(Object.isFrozen(config.pcpSyncStart));
  assert.equal(existsSync(path.join(f.state, "message-queue.db")), false);
});

test("absent inputs leave the ordinary helper inactive and cannot be attached by changing the environment later", async t => {
  const f = fixture(t);
  delete process.env.MATRIX_PCP_STARTUP_FILE; delete process.env.MATRIX_PCP_STARTUP_SHA256;
  const helper = await configuredHelper(f.root);
  process.env.MATRIX_PCP_STARTUP_FILE = f.file;
  process.env.MATRIX_PCP_STARTUP_SHA256 = digest(readFileSync(f.file));
  await helper.createConfiguredMatrixClient(selection.homeserverUrl, selection.userId, "fabricated", "ordinary-cursor");
  assert.equal(Object.hasOwn(helper.calls[0], "pcpSync"), false);
  assert.equal(Object.hasOwn(helper.calls[0], "pcpSyncStart"), false);
  assert.equal(helper.calls[0].syncToken, "ordinary-cursor");
  assert.deepEqual(readdirSync(f.state), []);
});

test("the startup binding remains frozen and refuses a different requested client identity", async t => {
  const f = fixture(t), helper = await configuredHelper(f.root);
  f.write({ ...record, selection: { ...selection, policy: { ...selection.policy, room: "!other:example.com" } } });
  await helper.createConfiguredMatrixClient(selection.homeserverUrl, selection.userId, "fabricated");
  assert.deepEqual(helper.calls[0].pcpSync, selection);
  await assert.rejects(helper.createConfiguredMatrixClient(selection.homeserverUrl, "@other:example.com", "fabricated"), /authenticated client/);
  process.env.MATRIX_DEVICE_ID = "OTHER";
  await assert.rejects(helper.createConfiguredMatrixClient(selection.homeserverUrl, selection.userId, "fabricated"), /authenticated client/);
  assert.equal(helper.calls.length, 1);
});

test("partial inputs, unsafe files, changed bytes and a general HTTP/OAuth route cannot load a selection", async t => {
  const f = fixture(t), { loadPcpStartup } = await import("../src/matrix/pcp-startup.js");
  const env = { ...process.env };
  assert.equal(loadPcpStartup({}), undefined);
  for (const change of [
    { MATRIX_PCP_STARTUP_SHA256: undefined }, { MATRIX_PCP_STARTUP_FILE: undefined },
    { MATRIX_PCP_STARTUP_FILE: "relative.json" }, { MATRIX_PCP_STARTUP_SHA256: "wrong" },
    { MATRIX_RETAINED_READ: "false" }, { MCP_CHILD: "0" }, { ENABLE_OAUTH: "true" },
    { ENABLE_TOKEN_EXCHANGE: "true" }, { MATRIX_PCP_STARTUP_FILE: path.join(f.root, "missing") },
    { MATRIX_PCP_STARTUP_SHA256: "0".repeat(64) },
  ]) assert.throws(() => loadPcpStartup({ ...env, ...change }));
  chmodSync(f.file, 0o644); assert.throws(() => loadPcpStartup(env), /unsafe/); chmodSync(f.file, 0o600);
  const link = path.join(f.root, "link.json"); symlinkSync(f.file, link);
  assert.throws(() => loadPcpStartup({ ...env, MATRIX_PCP_STARTUP_FILE: link }), /unsafe/);
  writeFileSync(f.file, Buffer.alloc(32 * 1024 + 1, 32));
  assert.throws(() => loadPcpStartup(env), /unsafe/);
  assert.deepEqual(readdirSync(f.state), []);
});

test("strict private content cannot broaden a selection, replace its decision or leak invalid bytes in an error", async t => {
  const f = fixture(t), { loadPcpStartup } = await import("../src/matrix/pcp-startup.js");
  const variants = [
    { ...record, credentials: "PRIVATE_FIXTURE_SENTINEL" }, { ...record, schema: "other" },
    { ...record, selection: { ...selection, deviceId: "OTHER" } },
    { ...record, selection: { ...selection, userId: "@other:example.com" } },
    { ...record, selection: { ...selection, policy: { ...selection.policy, bridge: {} } } },
    { ...record, initialBoundary: null },
    { ...record, initialBoundary: { ...initialBoundary, selection_sha256: "0".repeat(64) } },
  ];
  for (const value of variants) {
    f.write(value);
    assert.throws(() => loadPcpStartup(), error => error instanceof Error &&
      /invalid/.test(error.message) && !error.message.includes("PRIVATE_FIXTURE_SENTINEL"));
  }
  writeFileSync(f.file, "{PRIVATE_FIXTURE_SENTINEL");
  process.env.MATRIX_PCP_STARTUP_SHA256 = digest(readFileSync(f.file));
  assert.throws(() => loadPcpStartup(), error => error instanceof Error && !error.message.includes("PRIVATE_FIXTURE_SENTINEL"));
  f.write({ schema: record.schema, selection });
  const noDecision = loadPcpStartup();
  assert.deepEqual(noDecision?.selection, selection); assert.equal(Object.hasOwn(noDecision!, "initialBoundary"), false);
  assert.deepEqual(readdirSync(f.state), []);
});

test("the actual retained launcher check validates pinned inputs without reading a queue or changing state", t => {
  const f = fixture(t);
  writeFileSync(path.join(f.state, "data-version"), String(CURRENT_DATA_VERSION));
  writeFileSync(path.join(f.state, "login-state.json"), JSON.stringify({
    userId: selection.userId, deviceId: selection.deviceId, accessToken: "fabricated" }), { mode: 0o600 });
  writeFileSync(path.join(f.state, getCryptoDatabasePrefix(selection.userId) + "__matrix-sdk-crypto.sqlite"), "fabricated metadata fixture");
  const before = Object.fromEntries(readdirSync(f.state).map(name => [name, digest(readFileSync(path.join(f.state, name)))]));
  const script = fileURLToPath(new URL("../scripts/vps-retained-read.mjs", import.meta.url));
  const check = (env: NodeJS.ProcessEnv) => spawnSync(process.execPath, [script, "--check"], { env, encoding: "utf8", timeout: 10_000 });
  const valid = check({ ...process.env });
  assert.equal(valid.status, 0, valid.stderr); assert.match(valid.stdout, /Pinned PCP selection/);
  const invalid = check({ ...process.env, MATRIX_PCP_STARTUP_SHA256: "0".repeat(64) });
  assert.equal(invalid.status, 78); assert.doesNotMatch(invalid.stderr, /PRIVATE_FIXTURE_SENTINEL|!selected/);
  const disabled = { ...process.env }; delete disabled.MATRIX_PCP_STARTUP_FILE; delete disabled.MATRIX_PCP_STARTUP_SHA256;
  const ordinary = check(disabled); assert.equal(ordinary.status, 0, ordinary.stderr); assert.doesNotMatch(ordinary.stdout, /Pinned PCP/);
  assert.deepEqual(Object.fromEntries(readdirSync(f.state).map(name => [name, digest(readFileSync(path.join(f.state, name)))])), before);
  assert.equal(existsSync(path.join(f.state, "message-queue.db")), false);
});
