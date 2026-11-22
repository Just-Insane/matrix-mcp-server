import { constants } from "node:fs";
import type { Readable } from "node:stream";
import { mkdir, lstat, open, rename, unlink, readFile, statfs } from "node:fs/promises";
import { join } from "node:path";
import { createHash, createDecipheriv, randomBytes } from "node:crypto";
import type { Response } from "node-fetch";
import { canonicalJson, digest, type AcquisitionMode } from "./pcp-sync-observation.js";

export const MEDIA_CHUNK_BYTES = 8 * 1024 * 1024;
export const MAX_MEDIA_ORIGINAL_BYTES = 5 * 1024 * 1024 * 1024;
export interface MediaOriginal { object_ref: string; sha256: string; byte_size: number; media_type: string; source_locator: string | null }
export interface MediaContext { room: string; mode: AcquisitionMode; itemId: string }
export interface MediaInfo { locator: string; encrypted: boolean; expectedDigest: string | null; expectedSize: number | null; file?: any; mediaType?: string }
export interface SpooledMedia { path: string; sha256: string; byte_size: number; media_type: string; chunks: string[] }
export interface PreparedMedia {
  parts: { locator: string; original: MediaOriginal; derivation?: { kind: "matrix-media-decryption"; source_locator: string; source_sha256: string } }[];
  pending: boolean; gaps: string[];
}
const mime = (value: unknown): string => typeof value === "string" && /^[\x21-\x7e]{1,128}$/.test(value) ? value : "application/octet-stream";
export const decryptedMediaLocator = (locator: string, sha: string) => "matrix-decrypted:sha256:" + digest(canonicalJson([locator, sha]));
async function privateFile(path: string, size?: number): Promise<void> {
  const info = await lstat(path);
  if (!info.isFile() || info.isSymbolicLink() || info.uid !== process.getuid!() || info.nlink !== 1 ||
      (info.mode & 0o077) !== 0 || size !== undefined && info.size !== size) throw new Error("Private Matrix media spool unavailable");
}
async function directory(path: string): Promise<void> {
  await mkdir(path, { mode: 0o700 }).catch(error => { if (error.code !== "EEXIST") throw error; });
  const info = await lstat(path);
  if (!info.isDirectory() || info.isSymbolicLink() || info.uid !== process.getuid!() || (info.mode & 0o077) !== 0) throw new Error("Private Matrix media directory unavailable");
}
async function persist(path: string, value: unknown): Promise<void> {
  const temporary = path + "." + randomBytes(8).toString("hex") + ".partial";
  const file = await open(temporary, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW, 0o600);
  try { await file.writeFile(JSON.stringify(value)); await file.sync(); } finally { await file.close(); }
  await rename(temporary, path);
}
async function inspect(path: string, mediaType: string): Promise<SpooledMedia> {
  await privateFile(path);
  const full = createHash("sha256"), chunks: string[] = [];
  const file = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW);
  let size = 0;
  try {
    const info = await file.stat();
    if (!info.size || info.size > MAX_MEDIA_ORIGINAL_BYTES) throw new Error("Matrix media exceeds R2 original limit");
    for (let position = 0; position < info.size; position += MEDIA_CHUNK_BYTES) {
      const bytes = Buffer.alloc(Math.min(MEDIA_CHUNK_BYTES, info.size - position)); let offset = 0;
      while (offset < bytes.length) {
        const read = await file.read(bytes, offset, bytes.length - offset, position + offset);
        if (!read.bytesRead) throw new Error("Matrix media spool incomplete"); offset += read.bytesRead;
      }
      full.update(bytes); size += bytes.length; chunks.push(digest(bytes));
    }
  } finally { await file.close(); }
  return { path, sha256: full.digest("hex"), byte_size: size, media_type: mime(mediaType), chunks };
}

/** One bounded background media transfer belongs to the existing collector.
 * Live publication never waits for a full download. Private local custody and
 * immutable remote chunks let the same job resume after response loss/restart. */
export class PcpMediaTransfers {
  private active: Promise<void> | null = null;
  private readonly controller = new AbortController();
  private readonly results = new Map<string, PreparedMedia>();
  constructor(private readonly root: string, private readonly selectionDigest: string,
    private readonly download: (locator: string, signal: AbortSignal) => Promise<Response>,
    private readonly upload: (context: MediaContext, locator: string, file: SpooledMedia, signal: AbortSignal, canContinue: () => boolean) => Promise<MediaOriginal>,
    private readonly canRun: () => boolean) {}

  stop(): void { this.controller.abort(); }
  async settled(): Promise<void> { await this.active; }

  async prepare(context: MediaContext, info: MediaInfo): Promise<PreparedMedia> {
    const key = digest(canonicalJson(["matrix-media-spool-v1", this.selectionDigest, context.room, context.itemId, info.locator]));
    const cached = this.results.get(key);
    if (cached && !cached.pending) return cached;
    if (!this.active && !this.controller.signal.aborted && this.canRun()) {
      this.active = this.run(key, context, info).catch(() => {
        this.results.set(key, { parts: this.results.get(key)?.parts ?? [], pending: true, gaps: ["attachment_download_unavailable"] });
      }).finally(() => { this.active = null; });
    }
    return cached ?? { parts: [], pending: true, gaps: ["attachment_bytes_unavailable"] };
  }

  private async run(key: string, context: MediaContext, info: MediaInfo): Promise<void> {
    await directory(this.root); const dir = join(this.root, key); await directory(dir);
    const journal = join(dir, "transfer.json"), path = join(dir, "original.bin"), decodedPath = join(dir, "decoded.bin");
    let saved: any = null;
    try { await privateFile(journal); saved = JSON.parse(await readFile(journal, "utf8")); }
    catch (error: any) { if (error.code !== "ENOENT") throw error; }
    if (saved && (saved.schema !== "ea-matrix-media-spool-v1" || saved.binding !== key)) throw new Error("Matrix media spool binding changed");
    if (saved?.state === "retained") { this.results.set(key, saved.result); return; }
    const remember = async (value: any) => { saved = { ...saved, schema: "ea-matrix-media-spool-v1", binding: key, ...value }; await persist(journal, saved); };
    let original: SpooledMedia;
    try { original = await inspect(path, saved?.original?.media_type ?? (info.encrypted ? "application/octet-stream" : mime(info.mediaType))); }
    catch (error: any) {
      if (error.code !== "ENOENT") throw error;
      if (info.expectedSize !== null && info.expectedSize > MAX_MEDIA_ORIGINAL_BYTES) {
        this.results.set(key, { parts: [], pending: true, gaps: ["attachment_platform_size_limit"] }); return;
      }
      const temporary = join(dir, "download.partial");
      try { await privateFile(temporary); await unlink(temporary); } catch (error: any) { if (error.code !== "ENOENT") throw error; }
      const controller = new AbortController(), timer = setTimeout(() => controller.abort(), 600_000);
      const signal = AbortSignal.any([this.controller.signal, controller.signal]);
      let file: Awaited<ReturnType<typeof open>> | undefined;
      try {
        const response = await this.download(info.locator, signal);
        if (!response.ok || !response.body) throw new Error("Matrix media download unavailable");
        const declared = response.headers.get("content-length");
        const expected = declared !== null && /^\d+$/.test(declared) ? Number(declared) : info.expectedSize;
        const disk = await statfs(dir);
        if (expected !== null && (expected > MAX_MEDIA_ORIGINAL_BYTES || disk.bavail * disk.bsize < expected + 256 * 1024 * 1024)) {
          (response.body as Readable).destroy(); throw new Error("Matrix media spool capacity unavailable");
        }
        file = await open(temporary, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW, 0o600);
        let size = 0, checked = 0;
        try {
          for await (const chunk of response.body) {
            const bytes = Buffer.from(chunk); size += bytes.length;
            if (size > MAX_MEDIA_ORIGINAL_BYTES) throw new Error("Matrix media exceeds R2 original limit");
            if (size - checked >= 32 * 1024 * 1024) {
              const available = await statfs(dir); checked = size;
              if (available.bavail * available.bsize < 256 * 1024 * 1024) throw new Error("Matrix media spool capacity unavailable");
            }
            await file.writeFile(bytes);
          }
        } finally { (response.body as Readable).destroy(); }
        if (!size || expected !== null && size !== expected) throw new Error("Matrix media download incomplete");
        await file.sync(); await file.close(); file = undefined; await rename(temporary, path);
        const type = info.encrypted ? "application/octet-stream" : mime(info.mediaType ?? response.headers.get("content-type")?.split(";")[0]);
        original = await inspect(path, type);
      } finally { clearTimeout(timer); controller.abort(); await file?.close().catch(() => {}); }
    }
    if (info.expectedDigest && original.sha256 !== info.expectedDigest) throw new Error("Matrix ciphertext digest differs");
    await remember({ state: "spooled", original: { ...original, path: undefined } });
    const parts: PreparedMedia["parts"] = [];
    const retained = saved.retained_original ?? await this.upload(context, info.locator, original, this.controller.signal, this.canRun);
    await remember({ retained_original: retained }); parts.push({ locator: info.locator, original: retained });
    this.results.set(key, { parts, pending: info.encrypted, gaps: info.encrypted ? ["attachment_decryption_unavailable"] : [] });
    let decodeGap = false;
    if (info.encrypted) {
      const keyBytes = typeof info.file?.key?.k === "string" ? Buffer.from(info.file.key.k, "base64url") : Buffer.alloc(0);
      const iv = typeof info.file?.iv === "string" ? Buffer.from(info.file.iv, "base64") : Buffer.alloc(0);
      if (info.file?.v !== "v2" || info.file?.key?.kty !== "oct" || info.file?.key?.alg !== "A256CTR" ||
          keyBytes.length !== 32 || iv.length !== 16 || iv.subarray(8).some(byte => byte !== 0) || !info.expectedDigest) decodeGap = true;
      else {
        let clear: SpooledMedia;
        try { clear = await inspect(decodedPath, mime(info.mediaType)); }
        catch (error: any) {
          if (error.code !== "ENOENT") throw error;
          const capacity = await statfs(dir);
          if (capacity.bavail * capacity.bsize < original.byte_size + 256 * 1024 * 1024) throw new Error("Matrix decoded-media spool capacity unavailable");
          const temporary = join(dir, "decoded.partial");
          try { await privateFile(temporary); await unlink(temporary); } catch (error: any) { if (error.code !== "ENOENT") throw error; }
          const output = await open(temporary, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW, 0o600);
          const source = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW);
          const stream = source.createReadStream({ autoClose: false, highWaterMark: MEDIA_CHUNK_BYTES });
          const decipher = createDecipheriv("aes-256-ctr", keyBytes, iv);
          try {
            for await (const chunk of stream) {
              if (this.controller.signal.aborted) throw new Error("Matrix media stopped");
              await output.writeFile(decipher.update(chunk));
            }
            await output.writeFile(decipher.final()); await output.sync();
          } finally { keyBytes.fill(0); stream.destroy(); await source.close(); await output.close(); }
          await rename(temporary, decodedPath); clear = await inspect(decodedPath, mime(info.mediaType));
        }
        const locator = decryptedMediaLocator(info.locator, original.sha256);
        const decoded = saved.retained_decoded ?? await this.upload(context, locator, clear, this.controller.signal, this.canRun);
        await remember({ retained_decoded: decoded });
        parts.push({ locator, original: decoded, derivation: { kind: "matrix-media-decryption", source_locator: info.locator, source_sha256: original.sha256 } });
      }
      keyBytes.fill(0);
    }
    const result: PreparedMedia = { parts, pending: false, gaps: decodeGap ? ["attachment_decryption_unavailable"] : [] };
    await remember({ state: "retained", result }); this.results.set(key, result);
    // Only temporary transfer copies are removed; the exact verified original
    // and decoded rendition already have separate immutable R2 descriptors.
    await unlink(path).catch(error => { if (error.code !== "ENOENT") throw error; });
    await unlink(decodedPath).catch(error => { if (error.code !== "ENOENT") throw error; });
  }
}
