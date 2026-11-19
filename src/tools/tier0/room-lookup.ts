import { z } from "zod";
import { readRoomEvent, readRoomHistory, type LookupClient } from "../../matrix/room-lookup.js";
import { createConfiguredMatrixClient, getAccessToken, getMatrixContext } from "../../utils/server-helpers.js";
import { ToolRegistrationFunction } from "../../types/tool-types.js";

async function runLookup(context: any, read: (client: LookupClient) => Promise<unknown>) {
  try {
    const { matrixUserId, homeserverUrl } = getMatrixContext(context.requestInfo?.headers);
    const accessToken = getAccessToken(context.requestInfo?.headers, context.authInfo?.token);
    const client = await createConfiguredMatrixClient(homeserverUrl, matrixUserId, accessToken);
    return { content: [{ type: "text" as const, text: JSON.stringify(await read(client)) }] };
  } catch (error: any) {
    const known = ["ROOM_NOT_JOINED", "INVALID_LIMIT", "INVALID_EVENT_RESPONSE"];
    const code = known.includes(error?.message) ? error.message
      : typeof error?.errcode === "string" && /^M_[A-Z0-9_]+$/.test(error.errcode) ? error.errcode
      : "LOOKUP_FAILED";
    return {
      isError: true,
      content: [{ type: "text" as const, text: JSON.stringify({
        status: "unavailable", code,
        message: "The requested source read did not complete. This is not a no-match result.",
      }) }],
    };
  }
}

export const registerRoomLookupTools: ToolRegistrationFunction = (server) => {
  const roomId = z.string().min(1).describe("ID of an already-joined, authorized Matrix room");
  const annotations = { readOnlyHint: true, idempotentHint: true, openWorldHint: false };
  server.registerTool("get-room-history", {
    title: "Get Matrix Room History Page",
    description: "Read one bounded page of original events from an already-joined room, newest first. " +
      "Returns native event IDs, exact matrix.to links, missing-key status, and a continuation cursor. " +
      "Pass coverage.nextCursor as from to continue the same room. Empty pages can have more history; " +
      "endOfAccessibleHistory means this client's visibility limit, not that older records never existed. " +
      "Stop on paginationStalled. No message-text search or new room membership.",
    inputSchema: {
      roomId,
      from: z.string().min(1).optional().describe("Native continuation from the previous page for this room; omit for latest"),
      limit: z.coerce.number().int().min(1).max(100).default(50).describe("Maximum events in this page (1–100)"),
    }, annotations,
  }, (args: { roomId: string; from?: string; limit: number }, context: any) =>
    runLookup(context, client => readRoomHistory(client, args)));
  server.registerTool("get-room-event", {
    title: "Reopen a Matrix Event",
    description: "Reopen one original event by room ID and event ID in an already-joined room. " +
      "Returns its exact matrix.to link and distinguishes redacted, undecryptable, and available content. " +
      "Does not claim complete thread or edit history; does not download attachments.",
    inputSchema: { roomId, eventId: z.string().min(1).describe("Exact native event ID returned by a prior source read") },
    annotations,
  }, (args: { roomId: string; eventId: string }, context: any) =>
    runLookup(context, client => readRoomEvent(client, args.roomId, args.eventId)));
};
