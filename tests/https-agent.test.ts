import assert from "node:assert/strict";
import test from "node:test";
import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createServer } from "node:https";
import fetch from "node-fetch";
import { createMatrixHttpsAgent } from "../src/matrix/https-agent.js";

test("Matrix HTTPS verifies trust and hostname while accepting a configured CA", { timeout: 10000 }, async (t) => {
  const dir = mkdtempSync(join(tmpdir(), "matrix-tls-test-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  const config = join(dir, "cert.cnf");
  writeFileSync(config, "[req]\ndistinguished_name=dn\nx509_extensions=ext\nprompt=no\n[dn]\nCN=localhost\n[ext]\nsubjectAltName=IP:127.0.0.1\n");
  execFileSync("openssl", ["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1", "-config", config,
    "-keyout", join(dir, "key.pem"), "-out", join(dir, "cert.pem")], { stdio: "ignore" });
  const cert = readFileSync(join(dir, "cert.pem"));
  const server = createServer({ key: readFileSync(join(dir, "key.pem")), cert }, (_request, response) => {
    response.writeHead(200); response.end("synthetic versions");
  });
  await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
  t.after(() => { server.closeAllConnections(); server.close(); });
  const address = server.address();
  assert.ok(address && typeof address !== "string");
  const url = `https://127.0.0.1:${address.port}/_matrix/client/versions`;
  const untrusted = createMatrixHttpsAgent();
  t.after(() => untrusted.destroy());
  await assert.rejects(fetch(url, { agent: untrusted }), (error: any) => error.code === "DEPTH_ZERO_SELF_SIGNED_CERT");

  // Supply a synthetic root only to this test agent. Production uses Node's configured trust store.
  const trusted = createMatrixHttpsAgent();
  trusted.options.ca = cert;
  t.after(() => trusted.destroy());
  assert.equal((await fetch(url, { agent: trusted })).status, 200);

  const wrongName = createMatrixHttpsAgent();
  wrongName.options.ca = cert;
  wrongName.options.servername = "different.example.invalid";
  t.after(() => wrongName.destroy());
  await assert.rejects(fetch(url, { agent: wrongName }), (error: any) => error.code === "ERR_TLS_CERT_ALTNAME_INVALID");
});
