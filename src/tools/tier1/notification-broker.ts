import { z } from "zod";
import { ToolRegistrationFunction } from "../../types/tool-types.js";
import { sendMessageHandler } from "./messaging.js";

const MAX_NOTIFICATION_LENGTH = 8_000;

function configuredRoomId(): string {
  const roomId = process.env.MATRIX_NOTIFICATION_ROOM_ID?.trim();
  if (!roomId || !roomId.startsWith("!")) {
    throw new Error("MATRIX_NOTIFICATION_ROOM_ID must be a Matrix room ID");
  }
  return roomId;
}

export const registerNotificationBrokerTool: ToolRegistrationFunction = (server) => {
  server.registerTool(
    "send-notification",
    {
      title: "Send Matrix Notification",
      description:
        "Send one plain-text notification from the dedicated bot identity to its fixed, encrypted operator room. " +
        "The destination, Matrix identity, and homeserver are runtime-controlled and cannot be selected by callers.",
      inputSchema: {
        message: z
          .string()
          .min(1)
          .max(MAX_NOTIFICATION_LENGTH)
          .describe("Plain-text notification body, up to 8,000 characters"),
      },
      annotations: {
        readOnlyHint: false,
        destructiveHint: false,
        idempotentHint: false,
        openWorldHint: true,
      },
    },
    async ({ message }: { message: string }, context: any) =>
      sendMessageHandler(
        {
          roomId: configuredRoomId(),
          message,
          messageType: "text",
        },
        context
      )
  );
};
