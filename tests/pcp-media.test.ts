import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm, readdir, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createCipheriv } from "node:crypto";
import fetch, { Response } from "node-fetch";
import { PcpMediaTransfers, MEDIA_CHUNK_BYTES } from "../src/matrix/pcp-media.js";
import { PcpReceiver } from "../src/matrix/pcp-publication.js";
import { canonicalJson, digest, type PcpSyncSelection } from "../src/matrix/pcp-sync-observation.js";

const selection: PcpSyncSelection = {
 homeserverUrl: "https://matrix.example.com", userId: "@owner:example.com", deviceId: "fixture-device",
 receiverUrl: "https://pcp.example.com/v1/source/matrix-discord",
 policy: { schema: "ea-chat-source-v2", source: "matrix", account: "fixture-account", profile: "personal",
  room_scope: "all_joined", mode: "live_and_historical", principal: "service:fixture-client", policy_version: "existing", basis: "fixture" },
};
test("large encrypted originals resume after a lost finalization and retain a separate full decoded file", async t => {
 const temp = await mkdtemp(join(tmpdir(), "matrix-media-test-")), directory = join(temp, "spool");
 t.after(async () => { await rm(temp, { recursive: true, force: true }); });
 const plain = Buffer.alloc(17 * 1024 * 1024 + 73); for (let i = 0; i < plain.length; i++) plain[i] = i % 251;
 const key = Buffer.alloc(32, 71), iv = Buffer.concat([Buffer.alloc(8, 11), Buffer.alloc(8)]);
 const cipher = createCipheriv("aes-256-ctr", key, iv), encrypted = Buffer.concat([cipher.update(plain), cipher.final()]);
 const locator = "mxc://example.com/native-original", context = { room: "!room:example.com", mode: "historical" as const, itemId: "$native" };
 const info = { locator, encrypted: true, expectedDigest: digest(encrypted), expectedSize: encrypted.length, mediaType: "application/pdf",
  file: { v: "v2", key: { kty: "oct", alg: "A256CTR", k: key.toString("base64url"), ext: true, key_ops: ["encrypt", "decrypt"] },
   iv: iv.toString("base64"), hashes: { sha256: Buffer.from(digest(encrypted), "hex").toString("base64") } } };
 const complete = new Map<string, any>(), chunks = new Map<string, Buffer>();
 let downloads = 0, writes = 0, finalizations = 0, lost = false, allowed = false;
 const receiver = new PcpReceiver(selection, { clientId: "fixture-client", clientSecret: "fabricated" }, (async (url, init) => {
  assert.equal(String(url), selection.receiverUrl); assert.equal(init!.redirect, "error");
  const h = init!.headers as Record<string, string>;
  assert.equal(h["x-ea-operation"], "original"); assert.equal(h["x-ea-item-id"], "$native");
  const nativeItem = canonicalJson([selection.policy.profile, h["x-ea-room"], h["x-ea-item-id"]]);
  const scope = digest(canonicalJson(["ea-original-upload-v1", ["matrix", selection.policy.account, nativeItem, h["x-ea-media-id"]]]));
  const sha = h["x-ea-original-sha256"], object_ref = "r2://ea-raw/sources/common/streamed-originals/v1/" + scope + "/" + sha;
  const original = { object_ref, sha256: sha, byte_size: Number(h["x-ea-original-size"]), media_type: h["x-ea-original-media-type"], source_locator: null };
  const reply = (value: unknown) => new Response(JSON.stringify(value));
  if (h["x-ea-original-operation"] === "status") return reply(complete.has(object_ref) ? { status: "retained", original } : { status: "absent" });
  if (h["x-ea-original-operation"] === "chunk") {
   const index = Number(h["x-ea-chunk-index"]), partKey = object_ref + "/" + index;
   if (init!.method === "PUT") {
    const body = init!.body as Buffer; assert.ok(body.length <= MEDIA_CHUNK_BYTES); assert.equal(body.length, Number(h["Content-Length"]));
    assert.equal(digest(body), h["x-ea-chunk-sha256"]); assert.ok(!chunks.has(partKey)); chunks.set(partKey, body); writes++;
   }
   return reply(chunks.has(partKey) ? { status: "retained", part: index, sha256: digest(chunks.get(partKey)!), byte_size: chunks.get(partKey)!.length } : { status: "absent" });
  }
  const partDigests = JSON.parse(init!.body as string).chunks;
  const bytes = Buffer.concat(partDigests.map((_s: string, i: number) => chunks.get(object_ref + "/" + i)!));
  assert.equal(bytes.length, original.byte_size); assert.equal(digest(bytes), original.sha256);
  complete.set(object_ref, { original, bytes }); finalizations++;
  if (!lost) { lost = true; throw new Error("fixture lost completed response"); }
  return reply({ status: "retained", original });
 }) as typeof fetch);
 const make = () => new PcpMediaTransfers(directory, digest(canonicalJson(selection)), async () => { downloads++; return new Response(encrypted, { headers: { "content-length": String(encrypted.length) } }); },
  (...args) => receiver.uploadOriginal(...args), () => allowed);
 let transfers = make();
 assert.equal((await transfers.prepare(context, info)).pending, true); await transfers.settled(); assert.equal(downloads, 0);
 allowed = true; await transfers.prepare(context, info); await transfers.settled();
 assert.equal(downloads, 1); assert.equal(finalizations, 1); assert.equal(writes, 3);
 transfers.stop(); transfers = make();
 await transfers.prepare(context, info); await transfers.settled();
 const result = await transfers.prepare(context, info); assert.equal(result.pending, false); assert.deepEqual(result.gaps, []);
 assert.equal(downloads, 1); assert.equal(finalizations, 2); assert.equal(writes, 6);
 assert.equal(result.parts.length, 2); assert.equal(result.parts[0].locator, locator);
 assert.equal(result.parts[0].original.sha256, digest(encrypted)); assert.equal(result.parts[1].original.sha256, digest(plain));
 assert.equal(result.parts[1].derivation?.source_sha256, digest(encrypted));
 assert.deepEqual(complete.get(result.parts[0].original.object_ref).bytes, encrypted);
 assert.deepEqual(complete.get(result.parts[1].original.object_ref).bytes, plain);
 const [child] = await readdir(directory), journal = await readFile(join(directory, child, "transfer.json"), "utf8");
 assert.ok(!journal.includes(key.toString("base64url"))); assert.deepEqual(await readdir(join(directory, child)), ["transfer.json"]);
 transfers.stop();
});
