import { CallToolResult } from "@modelcontextprotocol/sdk/types.js";
import { z } from "zod";
import { createConfiguredMatrixClient, getAccessToken, getMatrixContext } from "../../utils/server-helpers.js";
import { removeClientFromCache } from "../../matrix/client.js";
import { getMessageQueue } from "../../matrix/messageQueue.js";
import { shouldEvictClientCache } from "../../utils/matrix-errors.js";
import { processMessage, processMessagesByDate, countMessagesByUser } from "../../matrix/messageProcessor.js";
import { ToolRegistrationFunction } from "../../types/tool-types.js";
import { backfillRoomHistoryUntil } from "../../matrix/history-coverage.js";

// Tool: Get room messages
export const getRoomMessagesHandler = async (
  { roomId, limit }: { roomId: string; limit: number },
  { requestInfo, authInfo }: any
) => {
  const { matrixUserId, homeserverUrl } = getMatrixContext(requestInfo?.headers);
  const accessToken = getAccessToken(requestInfo?.headers, authInfo?.token);
  
  try {
    const client = await createConfiguredMatrixClient(homeserverUrl, matrixUserId, accessToken);

    const room = client.getRoom(roomId);
    if (!room) {
      return {
        content: [
          {
            type: "text",
            text: `Error: Room with ID ${roomId} not found. You may not be a member of this room.`,
          },
        ],
        isError: true,
      };
    }

    const messages = await Promise.all(
      room
        .getLiveTimeline()
        .getEvents()
        .slice(-limit)
        .map((event) => processMessage(event, client))
    );

    const validMessages = messages.filter((message) => message !== null);

    return {
      content:
        validMessages.length > 0
          ? validMessages
          : [
              {
                type: "text",
                text: `No messages found in room ${room.name || roomId}`,
              },
            ],
    };
  } catch (error: any) {
    console.error(`Failed to get room messages: ${error.message}`);
    if (shouldEvictClientCache(error)) removeClientFromCache(matrixUserId, homeserverUrl);
    return {
      content: [
        {
          type: "text",
          text: `Error: Failed to get room messages - ${error.message}`,
        },
      ],
      isError: true,
    };
  }
};

// Tool: Get messages by date
export const getMessagesByDateHandler = async (
  { roomId, startDate, endDate }: { roomId: string; startDate: string; endDate: string },
  { requestInfo, authInfo }: any
) => {
  const { matrixUserId, homeserverUrl } = getMatrixContext(requestInfo?.headers);
  const accessToken = getAccessToken(requestInfo?.headers, authInfo?.token);
  
  try {
    const startTs = Date.parse(startDate);
    const endTs = Date.parse(endDate);
    if (!Number.isFinite(startTs) || !Number.isFinite(endTs) || startTs > endTs) {
      throw new Error("Invalid date range: supply a valid start at or before the end.");
    }
    const client = await createConfiguredMatrixClient(homeserverUrl, matrixUserId, accessToken);
    const room = client.getRoom(roomId);
    if (!room) {
      return {
        content: [
          {
            type: "text",
            text: `Error: Room with ID ${roomId} not found. You may not be a member of this room.`,
          },
        ],
        isError: true,
      };
    }

    getMessageQueue().setRoomPaginationToken(room.roomId, room.oldState.paginationToken);
    const history = await backfillRoomHistoryUntil(client, room, startTs, () =>
      getMessageQueue().setRoomPaginationToken(room.roomId, room.oldState.paginationToken));

    const events = room.getLiveTimeline().getEvents();
    const messages = await processMessagesByDate(events, startDate, endDate, client);
    const dates = events.map(event => event.getTs()).filter(Number.isFinite).sort((a, b) => a - b);
    const encryptedUnavailable = events.filter(event => event.getTs() >= startTs && event.getTs() <= endTs &&
      (event.isDecryptionFailure() || event.getType() === "m.room.encrypted")).length;
    const historyCoverage = {
      type: "history_coverage", roomId, startDate, endDate,
      scope: "loaded_live_timeline", ...history,
      earliestLoadedTimestamp: dates.length ? new Date(dates[0]).toISOString() : null,
      latestLoadedTimestamp: dates.length ? new Date(dates[dates.length - 1]).toISOString() : null,
      encryptedUnavailable,
      note: "A bounded timeline read is not a guarantee of complete provider history. Continue with get-room-history if needed; missing keys are not a no-match result.",
    };

    return {
      content: [{ type: "text" as const, text: JSON.stringify(historyCoverage) }, ...(
        messages.length > 0
          ? messages
          : [
              {
                type: "text" as const,
                text: "No message was returned from the loaded timeline in this window. Check history_coverage before concluding there is no match.",
              },
            ])],
    };
  } catch (error: any) {
    console.error(`Failed to filter messages by date: ${error.message}`);
    if (shouldEvictClientCache(error)) removeClientFromCache(matrixUserId, homeserverUrl);
    return {
      content: [
        {
          type: "text",
          text: `Error: Failed to filter messages by date - ${error.message}`,
        },
      ],
      isError: true,
    };
  }
};

// Tool: Identify active users
export const identifyActiveUsersHandler = async (
  { roomId, limit }: { roomId: string; limit: number },
  { requestInfo, authInfo }: any
) => {
  const { matrixUserId, homeserverUrl } = getMatrixContext(requestInfo?.headers);
  const accessToken = getAccessToken(requestInfo?.headers, authInfo?.token);
  
  try {
    const client = await createConfiguredMatrixClient(homeserverUrl, matrixUserId, accessToken);
    const room = client.getRoom(roomId);
    if (!room) {
      return {
        content: [
          {
            type: "text",
            text: `Error: Room with ID ${roomId} not found. You may not be a member of this room.`,
          },
        ],
        isError: true,
      };
    }

    const events = room.getLiveTimeline().getEvents();
    const activeUsers = countMessagesByUser(events, limit);

    return {
      content:
        activeUsers.length > 0
          ? activeUsers.map((user) => ({
              type: "text",
              text: `${user.userId}: ${user.count} messages`,
            }))
          : [
              {
                type: "text",
                text: `No message activity found in room ${room.name || roomId}`,
              },
            ],
    };
  } catch (error: any) {
    console.error(`Failed to identify active users: ${error.message}`);
    if (shouldEvictClientCache(error)) removeClientFromCache(matrixUserId, homeserverUrl);
    return {
      content: [
        {
          type: "text",
          text: `Error: Failed to identify active users - ${error.message}`,
        },
      ],
      isError: true,
    };
  }
};

// Registration function
export const registerMessageTools: ToolRegistrationFunction = (server) => {
  // Tool: Get room messages
  server.registerTool(
    "get-room-messages",
    {
      title: "Get Matrix Room Messages",
      description:
        "Retrieve recent messages from a specific Matrix room, including text and image content. " +
        "Each text message is returned as a JSON object with fields: eventId, sender, timestamp, body, " +
        "and optionally replyToEventId and threadRootEventId. " +
        "Use eventId with send-message's replyToEventId to reply to a message, " +
        "or with threadRootEventId to continue a thread.",
      inputSchema: {
        roomId: z.string().describe("Matrix room ID (e.g., !roomid:domain.com)"),
        limit: z.coerce
          .number()
          .default(20)
          .describe("Maximum number of messages to retrieve (default: 20)"),
      },
      annotations: { readOnlyHint: true, idempotentHint: true, openWorldHint: false },
    },
    getRoomMessagesHandler
  );

  // Tool: Get messages by date
  server.registerTool(
    "get-messages-by-date",
    {
      title: "Get Matrix Messages by Date Range",
      description:
        "Retrieve messages from a Matrix room within a specific date range. " +
        "Each text message is a JSON object with: eventId, sender, timestamp, body, " +
        "and optionally replyToEventId and threadRootEventId.",
      inputSchema: {
        roomId: z.string().describe("Matrix room ID (e.g., !roomid:domain.com)"),
        startDate: z
          .string()
          .describe("Start date in ISO 8601 format (e.g., 2024-01-01T00:00:00Z)"),
        endDate: z
          .string()
          .describe("End date in ISO 8601 format (e.g., 2024-01-02T00:00:00Z)"),
      },
      annotations: { readOnlyHint: true, idempotentHint: true, openWorldHint: false },
    },
    getMessagesByDateHandler
  );

  // Tool: Identify active users
  server.registerTool(
    "identify-active-users",
    {
      title: "Identify Most Active Users",
      description: "Find the most active users in a Matrix room based on message count in recent history",
      inputSchema: {
        roomId: z.string().describe("Matrix room ID (e.g., !roomid:domain.com)"),
        limit: z.coerce
          .number()
          .default(10)
          .describe("Maximum number of active users to return (default: 10)"),
      },
      annotations: { readOnlyHint: true, idempotentHint: true, openWorldHint: false },
    },
    identifyActiveUsersHandler
  );
};
