import assert from "node:assert/strict";
import test from "node:test";
import type fetch from "node-fetch";
import { retainedReadFetch } from "../src/matrix/retained-read-fetch.js";

test("retained transport blocks SDK key backup restore, discovery, and upload before network access", async () => {
  let calls = 0;
  const wrapped = retainedReadFetch((async () => { calls++; return {} as any; }) as typeof fetch);
  for (const path of ["/room_keys/version", "/room_keys/keys/room/session", "/room_keys", "/%72oom_keys/version"]) {
    for (const method of ["GET", "PUT", "DELETE"]) {
      await assert.rejects(wrapped(`https://matrix.example.com/_matrix/client/v3${path}`, { method }), /backup restore and upload are disabled/);
    }
  }
  assert.equal(calls, 0);
});

test("retained transport keeps native request headers and refuses credential-bearing redirects", async () => {
  let captured: any;
  const wrapped = retainedReadFetch((async (input, init) => {
    captured = { input, init }; return {} as any;
  }) as typeof fetch);
  const headers = { Authorization: "Bearer dummy", Accept: "application/json" };
  await wrapped("https://matrix.example.com/_matrix/client/v3/account/whoami", { headers, redirect: "follow" });
  assert.equal(captured.init.redirect, "error");
  assert.deepEqual(captured.init.headers, headers);
});
