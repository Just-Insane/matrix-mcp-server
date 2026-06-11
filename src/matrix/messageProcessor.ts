import * as sdk from "matrix-js-sdk";
import { MatrixClient, EventType } from "matrix-js-sdk";
import fetch from "node-fetch";

/**
 * Represents a processed message that can be returned to MCP clients.
 * Text messages include metadata (eventId, sender, timestamp, threading info)
 * as a structured JSON object so callers can use eventId for replies/threading.
 */
export type ProcessedMessage =
  | { type: "text"; text: string }
  | { type: "image"; data: string; mimeType: string };

/**
 * Processes a Matrix event and extracts relevant content
 * 
 * @param event - Matrix event to process
 * @param matrixClient - Matrix client instance for fetching additional data
 * @returns Promise<ProcessedMessage | null> - Processed message or null if not processable
 */
export async function processMessage(
  event: sdk.MatrixEvent,
  matrixClient: MatrixClient | null
): Promise<ProcessedMessage | null> {
  if (!matrixClient) {
    throw new Error("Matrix client is not initialized.");
  }

  const evtType = event.getType();
  // Handle both decrypted (m.room.message) and still-encrypted (m.room.encrypted) events
  const isMessage = evtType === EventType.RoomMessage || evtType === EventType.RoomMessageEncrypted;

  if (isMessage) {
    // Use decrypted content if available, fall back to raw content
    const content = event.getClearContent?.() || event.getContent();
    if (!content) return null;

    // Extract threading/relation metadata
    const relatesToRaw = content["m.relates_to"] as Record<string, any> | undefined;

    // Skip m.replace events (raw edit events — original updated in-place) and redacted events.
    if (relatesToRaw?.rel_type === "m.replace") return null;
    if (event.isRedacted()) return null;

    const replyToEventId = relatesToRaw?.["m.in_reply_to"]?.event_id as string | undefined;
    const threadRootEventId = (relatesToRaw?.rel_type === "m.thread" || relatesToRaw?.rel_type === "io.element.thread")
      ? (relatesToRaw.event_id as string | undefined)
      : undefined;

    const isEncrypted = evtType === EventType.RoomMessageEncrypted;
    if (event.isDecryptionFailure() || isEncrypted || content.msgtype === "m.text") {
      const body = content.body || (isEncrypted ? "[encrypted]" : "");
      const metadata: Record<string, any> = {
        eventId: event.getId(),
        sender: event.getSender(),
        timestamp: new Date(event.getTs()).toISOString(),
        body: String(body),
      };
      if (isEncrypted && !content.body) {
        metadata.decryptionFailed = true;
        const reason = (event as any).decryptionFailureReason;
        if (reason) metadata.decryptionFailureReason = reason;
      }
      if (replyToEventId) metadata.replyToEventId = replyToEventId;
      if (threadRootEventId) metadata.threadRootEventId = threadRootEventId;

      return {
        type: "text",
        text: JSON.stringify(metadata),
      };
    } else if (content.msgtype === "m.image" && content.url) {
      try {
        const httpUrl = String(matrixClient.mxcUrlToHttp(content.url) || "");
        const response = await fetch(httpUrl);
        
        if (!response.ok) {
          throw new Error(`Failed to fetch image: ${response.statusText}`);
        }
        
        const buffer = await response.arrayBuffer();
        const base64Data = Buffer.from(buffer).toString("base64");
        
        return {
          type: "image",
          data: base64Data,
          mimeType: String(
            content.info?.mimetype || "application/octet-stream"
          ),
        };
      } catch (error: any) {
        console.error(`Failed to fetch image content: ${error.message}`);
        return null;
      }
    }
  }
  
  return null;
}

/**
 * Filters and processes messages within a date range
 * 
 * @param events - Array of Matrix events
 * @param startDate - Start date string
 * @param endDate - End date string
 * @param matrixClient - Matrix client instance
 * @returns Promise<ProcessedMessage[]> - Array of processed messages
 */
export async function processMessagesByDate(
  events: sdk.MatrixEvent[],
  startDate: string,
  endDate: string,
  matrixClient: MatrixClient
): Promise<ProcessedMessage[]> {
  const start = new Date(startDate).getTime();
  const end = new Date(endDate).getTime();

  const filteredEvents = events.filter((event) => {
    const timestamp = event.getTs();
    return timestamp >= start && timestamp <= end;
  });

  // Backfilled encrypted events often decrypt asynchronously after pagination.
  // Wait for decryption on the target window before serializing message bodies.
  await Promise.allSettled(
    filteredEvents
      .slice()
      .reverse()
      .map((event) => matrixClient.decryptEventIfNeeded(event))
  );

  const messages = await Promise.all(
    filteredEvents.map((event) => processMessage(event, matrixClient))
  );

  return messages.filter((message) => message !== null) as ProcessedMessage[];
}

/**
 * Counts messages by user in a room
 * 
 * @param events - Array of Matrix events
 * @param limit - Maximum number of users to return
 * @returns Array of user message counts
 */
export function countMessagesByUser(
  events: sdk.MatrixEvent[],
  limit: number = 10
): Array<{ userId: string; count: number }> {
  const userMessageCounts: Record<string, number> = {};
  
  events
    .filter((event) => event.getType() === EventType.RoomMessage || event.getType() === EventType.RoomMessageEncrypted)
    .forEach((event) => {
      const sender = event.getSender();
      if (sender) {
        userMessageCounts[sender] = (userMessageCounts[sender] || 0) + 1;
      }
    });

  return Object.entries(userMessageCounts)
    .sort(([, countA], [, countB]) => countB - countA)
    .slice(0, limit)
    .map(([userId, count]) => ({ userId, count }));
}
