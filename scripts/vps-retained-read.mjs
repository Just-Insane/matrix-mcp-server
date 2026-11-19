#!/usr/bin/env node
import { fileURLToPath } from "node:url";
import { retainedReadEnvironment } from "../dist/matrix/retained-session.js";

// The existing host credential wrapper supplies these fields privately.
// Never put an access token in the registry configuration or command arguments.
try {
  if (typeof process.execve !== "function") throw new Error("Node with execve is required");
  if (process.argv.length > 3 || (process.argv[2] && process.argv[2] !== "--check")) {
    throw new Error("Only --check is supported");
  }
  const env = retainedReadEnvironment({
    dataDir: process.env.MATRIX_DATA_DIR ?? "",
    homeserverUrl: process.env.MATRIX_HOMESERVER_URL ?? "",
    userId: process.env.MATRIX_USER_ID ?? "",
  }, process.env, {
    accessToken: process.env.MATRIX_ACCESS_TOKEN ?? "",
    deviceId: process.env.MATRIX_DEVICE_ID ?? "",
  });
  if (process.argv[2] === "--check") {
    console.log("Startup credentials match the retained Matrix session and current store metadata; no network or store changes");
    process.exit(0);
  }
  const entrypoint = fileURLToPath(new URL("../dist/stdio-server.js", import.meta.url));
  process.execve(process.execPath, [process.execPath, entrypoint],
    Object.fromEntries(Object.entries(env).filter(([, value]) => typeof value === "string")));
} catch {
  console.error("Retained Matrix read launch failed: check private startup credentials and saved-session/store metadata; no credentials were changed");
  process.exitCode = 78;
}
