import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";

// Import tool registration functions
// Tier 0 (Read-only tools)
import { registerRoomTools } from "./tools/tier0/rooms.js";
import { registerMessageTools } from "./tools/tier0/messages.js";
import { registerUserTools } from "./tools/tier0/users.js";
import { registerSearchTools } from "./tools/tier0/search.js";
import { registerNotificationTools } from "./tools/tier0/notifications.js";
import { registerWaitForMessagesTools } from "./tools/tier0/wait-for-messages.js";
import { registerGetQueuedMessagesTools } from "./tools/tier0/get-queued-messages.js";
import { registerInviteTools } from "./tools/tier0/invites.js";
import { registerReplayQueueTools } from "./tools/tier0/replay-queue.js";

// Tier 1 (Action tools)
import { registerMessagingTools } from "./tools/tier1/messaging.js";
import { registerRoomManagementTools } from "./tools/tier1/room-management.js";
import { registerRoomAdminTools } from "./tools/tier1/room-admin.js";
import { registerMessageActionTools } from "./tools/tier1/message-actions.js";
import { registerServerAdminTools } from "./tools/tier1/server-admin.js";
import { registerThreadMessageTools } from "./tools/tier1/thread-messages.js";

/**
 * Create a protocol server instance for one MCP transport.
 *
 * McpServer instances are single-transport objects. The HTTP endpoint is
 * stateless, so it must create one server per request rather than reconnecting
 * the module-level stdio server. Matrix client state remains shared through
 * clientCache; only the MCP protocol object is request-scoped.
 */
export function createServer(): McpServer {
  const server = new McpServer(
    {
      name: "matrix-mcp-server",
      version: "1.0.0",
    },
    {
      capabilities: {
        logging: {},
        resources: {},
        tools: {},
      },
    }
  );

  // Tier 0: Read-only Matrix tools
  registerRoomTools(server);        // list-joined-rooms, get-room-info, get-room-members
  registerMessageTools(server);     // get-room-messages, get-messages-by-date, identify-active-users
  registerUserTools(server);        // get-user-profile, get-my-profile, get-all-users
  registerSearchTools(server);      // search-public-rooms
  registerNotificationTools(server); // get-notification-counts, get-direct-messages
  registerWaitForMessagesTools(server); // wait-for-messages
  registerGetQueuedMessagesTools(server); // get-queued-messages
  registerInviteTools(server);          // get-pending-invites
  registerReplayQueueTools(server);     // replay-queue

  // Tier 1: Action Matrix tools
  registerMessagingTools(server);       // send-message, send-direct-message
  registerRoomManagementTools(server);  // create-room, join-room, leave-room, invite-user
  registerRoomAdminTools(server);       // set-room-name, set-room-topic
  registerMessageActionTools(server);  // redact-event, send-reaction, edit-message
  registerServerAdminTools(server);    // restart-server
  registerThreadMessageTools(server);  // get-thread-messages

  return server;
}

// Preserve the package's existing default export for consumers that own a
// single long-lived transport. HTTP callers must use createServer().
export default createServer();
