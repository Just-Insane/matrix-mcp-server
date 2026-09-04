import assert from "node:assert/strict";
import { getEventListeners } from "node:events";
import test from "node:test";
import { createServer } from "node:http";
import fetch from "node-fetch";
import { boundedRequestSignal } from "../src/matrix/request-signal.js";

test("SDK cancellation aborts an in-flight bounded request", () => {
  const upstream = new AbortController();
  const scope = boundedRequestSignal(upstream.signal, 1000);
  assert.equal(scope.signal.aborted, false);
  upstream.abort();
  assert.equal(scope.signal.aborted, true);
  scope.dispose();
});

test("an already cancelled SDK request cannot start another sync", () => {
  const upstream = new AbortController();
  upstream.abort();
  const scope = boundedRequestSignal(upstream.signal, 1000);
  assert.equal(scope.signal.aborted, true);
  scope.dispose();
  assert.equal(getEventListeners(upstream.signal, "abort").length, 0);
});

test("network deadline remains bounded without SDK cancellation", async () => {
  const scope = boundedRequestSignal(undefined, 5);
  await new Promise<void>((resolve) => scope.signal.addEventListener("abort", () => resolve()));
  assert.equal(scope.signal.aborted, true);
  scope.dispose();
});

test("response headers cancel the deadline without adding SDK listeners", async () => {
  const upstream = new AbortController();
  const scope = boundedRequestSignal(upstream.signal, 5);
  assert.equal(getEventListeners(upstream.signal, "abort").length, 0);
  scope.dispose();
  scope.dispose();
  await new Promise((resolve) => setTimeout(resolve, 10));
  assert.equal(scope.signal.aborted, false);
  assert.equal(getEventListeners(upstream.signal, "abort").length, 0);
});

test("SDK cancellation still aborts body reads after response headers", () => {
  const upstream = new AbortController();
  const scope = boundedRequestSignal(upstream.signal, 1000);
  scope.dispose();
  upstream.abort();
  assert.equal(scope.signal.aborted, true);
});

for (const mode of ["SDK cancellation", "network deadline"] as const) {
  test(`node-fetch aborts a real HTTP request on ${mode}`, { timeout: 2000 }, async (t) => {
    const server = createServer();
    const received = new Promise<void>((resolve) => server.once("request", () => resolve()));
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    t.after(() => { server.closeAllConnections(); server.close(); });
    const address = server.address();
    assert.ok(address && typeof address !== "string");
    const upstream = new AbortController();
    const scope = boundedRequestSignal(upstream.signal, mode === "network deadline" ? 100 : 1000);
    t.after(scope.dispose);
    const rejected = assert.rejects(fetch(`http://127.0.0.1:${address.port}/sync`, { signal: scope.signal }), { name: "AbortError" });
    if (mode === "SDK cancellation") { await received; upstream.abort(); }
    await rejected;
  });
}

test("node-fetch body reads remain cancellable after headers and deadline disposal", { timeout: 2000 }, async (t) => {
  const server = createServer((_request, response) => {
    response.writeHead(200, { "Content-Type": "application/json" });
    response.write('{"unfinished":');
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  t.after(() => { server.closeAllConnections(); server.close(); });
  const address = server.address();
  assert.ok(address && typeof address !== "string");
  const upstream = new AbortController();
  const scope = boundedRequestSignal(upstream.signal, 1000);
  t.after(scope.dispose);
  const response = await fetch(`http://127.0.0.1:${address.port}/sync`, { signal: scope.signal });
  scope.dispose();
  const rejected = assert.rejects(response.text(), { name: "AbortError" });
  upstream.abort();
  await rejected;
});
