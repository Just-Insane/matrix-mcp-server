#!/usr/bin/env node
/**
 * Hot-reload architecture: outer process (wrapper) ↔ inner process (MCP server).
 * On restart (exit code 0): PID stays stable, stdio stays attached, env vars survive.
 * Re-initialized: Matrix SDK clients (fresh login + sync), MCP transport, tool list.
 * Persists across restarts: SQLite crypto stores, sync tokens (if stored), env config.
 */
import "dotenv/config";
import { spawn, ChildProcess } from "child_process";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { logger as matrixLogger } from "matrix-js-sdk/lib/logger.js";
import server from "./server.js";
import { shutdownAllClients } from "./matrix/clientCache.js";
import { startAutoSync, stopAutoSync } from "./matrix/autoSync.js";
import { closeMessageQueue } from "./matrix/messageQueue.js";

// Self-wrapping hot-reload: the outer process (no MCP_CHILD env) stays alive and
// restarts the inner process on clean exit (exit code 0). Claude Code's stdio
// connection stays attached to the outer process — no /mcp reconnect needed.
if (!process.env.MCP_CHILD) {
  let currentChild: ChildProcess | null = null;

  function killChild(signal: NodeJS.Signals) {
    if (currentChild) currentChild.kill(signal);
    process.exit(0);
  }
  process.on("SIGTERM", () => killChild("SIGTERM"));
  process.on("SIGINT", () => killChild("SIGINT"));

  (async () => {
    while (true) {
      currentChild = spawn(process.execPath, process.argv.slice(1), {
        stdio: "inherit",
        env: { ...process.env, MCP_CHILD: "1" },
      });
      const code: number = await new Promise<number>((resolve) =>
        currentChild!.on("close", (exitCode: number | null) => resolve(exitCode ?? 1))
      );
      currentChild = null;
      if (code !== 0) process.exit(code);
      // code === 0: hot reload requested — restart immediately
    }
  })().catch((err) => {
    process.stderr.write(`Wrapper error: ${err}\n`);
    process.exit(1);
  });
} else {
  // Inner process (MCP_CHILD=1): run the actual MCP server.

  // Stdout is reserved for MCP JSON-RPC. Redirect all console output methods to stderr.
  // matrix-js-sdk's loglevel factory calls console[methodName] at runtime, so we must
  // redirect debug/info/trace/log — not just log.
  console.log   = (...args: unknown[]) => process.stderr.write(args.join(" ") + "\n");
  console.info  = (...args: unknown[]) => process.stderr.write(args.join(" ") + "\n");
  console.debug = (...args: unknown[]) => process.stderr.write(args.join(" ") + "\n");
  console.trace = (...args: unknown[]) => process.stderr.write(args.join(" ") + "\n");

  // Also suppress matrix-js-sdk's verbose debug/info logging at the loglevel layer.
  // warn and error still go to stderr (console.warn/error are already on stderr).
  // setLevel is a loglevel method not exposed in the TS interface, so cast to any.
  (matrixLogger as any).setLevel("warn");

  const required = ["MATRIX_USER_ID", "MATRIX_ACCESS_TOKEN", "MATRIX_HOMESERVER_URL"] as const;
  const missing = required.filter((key) => !process.env[key]);
  if (missing.length > 0) {
    console.error(`Missing required environment variables: ${missing.join(", ")}`);
    console.error("Set them in your environment or in a .env file.");
    process.exit(1);
  }

  async function main() {
    const transport = new StdioServerTransport();
    await server.connect(transport);
    console.error("matrix-mcp-server running on stdio");
    // Start auto-sync loop — pushes Matrix events into the message queue
    startAutoSync().catch((err) => console.error("[autoSync] Failed to start:", err));
    // Notify Claude Code to re-fetch the tool list (handles hot reload).
    setTimeout(() => server.sendToolListChanged(), 100);
  }

  function shutdown() {
    console.error("Shutting down...");
    stopAutoSync();
    closeMessageQueue();
    shutdownAllClients();
    process.exit(0);
  }

  process.on("SIGTERM", shutdown);
  process.on("SIGINT", shutdown);

  main().catch((err) => {
    console.error("Fatal error:", err);
    process.exit(1);
  });
}
