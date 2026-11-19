#!/usr/bin/env node
import { execFileSync } from "node:child_process";
import { userInfo } from "node:os";
import { fileURLToPath } from "node:url";
import { retainedReadEnvironment } from "../dist/matrix/retained-session.js";

// Use existing Keychain configuration and the matching local login/crypto store.
// No secret is written, printed, or passed as a command-line argument.
try {
  if (process.platform !== "darwin" || typeof process.execve !== "function") {
    throw new Error("This launcher requires macOS and Node with execve support");
  }
  if (process.argv.length > 3 || (process.argv[2] && process.argv[2] !== "--check")) {
    throw new Error("Only --check is supported");
  }
  const account = userInfo().username;
  const readSetting = name => execFileSync("/usr/bin/security", [
    "find-generic-password", "-a", account, "-s", `mcp-matrix-${name}`, "-w",
  ], { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] }).trim();
  const env = retainedReadEnvironment({
    dataDir: readSetting("MATRIX_DATA_DIR"),
    homeserverUrl: readSetting("MATRIX_HOMESERVER_URL"),
    userId: readSetting("MATRIX_USER_ID"),
  });
  if (process.argv[2] === "--check") {
    console.log("Retained Matrix session and current crypto-store metadata are ready; no network or store changes");
    process.exit(0);
  }
  const entrypoint = fileURLToPath(new URL("../dist/stdio-server.js", import.meta.url));
  process.execve(process.execPath, [process.execPath, entrypoint],
    Object.fromEntries(Object.entries(env).filter(([, value]) => typeof value === "string")));
} catch {
  console.error("Retained Matrix read launch failed: check existing Keychain configuration and saved-session/store metadata; no credentials were changed");
  process.exitCode = 78;
}
