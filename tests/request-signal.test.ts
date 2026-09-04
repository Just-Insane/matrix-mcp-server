import assert from "node:assert/strict";
import { getEventListeners } from "node:events";
import test from "node:test";
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
