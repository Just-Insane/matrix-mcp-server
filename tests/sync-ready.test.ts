import assert from "node:assert/strict";
import test from "node:test";
import {
  INITIAL_SYNC_TIMEOUT_MS,
  isUsableSyncState,
  waitForUsableSync,
  type SyncStateValue,
} from "../src/matrix/sync-ready.js";

test("cold startup budget exceeds a sync request but remains bounded", () => {
  assert.ok(INITIAL_SYNC_TIMEOUT_MS > 65_000);
  assert.ok(INITIAL_SYNC_TIMEOUT_MS <= 120_000);
});

function syncHarness(initialState: SyncStateValue) {
  let state = initialState;
  const listeners = new Set<(next: SyncStateValue) => void>();
  return {
    getState: () => state,
    subscribe(listener: (next: SyncStateValue) => void) {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
    emit(next: SyncStateValue) {
      state = next;
      for (const listener of listeners) listener(next);
    },
    listenerCount: () => listeners.size,
  };
}

test("treats PREPARED and SYNCING as usable states", () => {
  assert.equal(isUsableSyncState("PREPARED"), true);
  assert.equal(isUsableSyncState("SYNCING"), true);
  assert.equal(isUsableSyncState("RECONNECTING"), false);
});

test("returns immediately when PREPARED was emitted before subscription", async () => {
  const harness = syncHarness("PREPARED");
  await waitForUsableSync(harness.getState, harness.subscribe, 25);
  assert.equal(harness.listenerCount(), 0);
});

test("ignores transitional states and resolves once sync becomes usable", async () => {
  const harness = syncHarness("RECONNECTING");
  const ready = waitForUsableSync(harness.getState, harness.subscribe, 100);
  harness.emit("CATCHUP");
  harness.emit("SYNCING");
  await ready;
  assert.equal(harness.listenerCount(), 0);
});

test("rechecks after subscribing to close the missed-event race", async () => {
  const harness = syncHarness("RECONNECTING");
  const subscribe = (listener: (next: SyncStateValue) => void) => {
    harness.emit("PREPARED");
    return harness.subscribe(listener);
  };
  await waitForUsableSync(harness.getState, subscribe, 25);
  assert.equal(harness.listenerCount(), 0);
});

test("times out and removes its listener when sync never becomes usable", async () => {
  const harness = syncHarness("RECONNECTING");
  await assert.rejects(
    waitForUsableSync(harness.getState, harness.subscribe, 10),
    /Matrix initial sync timed out/
  );
  assert.equal(harness.listenerCount(), 0);
});
