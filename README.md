# Matrix MCP Server

A comprehensive **Model Context Protocol (MCP) server** that provides secure access to Matrix homeserver functionality. Built with TypeScript, this server enables MCP clients to interact with Matrix rooms, messages, users, and more through a standardized interface — including full **end-to-end encrypted rooms**.

## Features

- 🔒 **End-to-End Encryption** — reads and writes E2EE rooms natively, with persistent crypto state across restarts
- 🔐 **OAuth 2.0 Authentication** with token exchange support
- 📱 **Matrix Tools** organized by functionality tiers
- 🔌 **Stdio & HTTP transports** — use via `npx` or as an HTTP server
- 🏠 **Multi-homeserver Support** with configurable endpoints
- ♻️ **Hot-reload** — update the server without dropping the MCP connection
- 🔄 **Sync token persistence** — resumes exactly where it left off after restart
- 🚀 **Production Ready** with comprehensive error handling
- 📊 **Rich Responses** with detailed Matrix data

## Prerequisites

- **Node.js 20+** and npm
- **Matrix homeserver** access (Synapse, Dendrite, etc.)
- **MCP client** (Claude Code, Codex, VS Code, etc.)

## Setup: Stdio (recommended)

The simplest way to use the server. Your MCP client launches it automatically via `npx` — no cloning, building, or running a server yourself. You just need three things from your Matrix homeserver: your user ID, an access token, and the homeserver URL.

### Claude Code

```bash
claude mcp add --scope user matrix-server \
  -e MATRIX_USER_ID=@you:your-homeserver.com \
  -e MATRIX_ACCESS_TOKEN=syt_... \
  -e MATRIX_HOMESERVER_URL=https://your-homeserver.com \
  -e MATRIX_DATA_DIR=/path/to/persistent/data \
  -e MATRIX_PASSWORD=your-matrix-password \
  -- npx github:Vegard-/matrix-mcp-server
```

### Codex

```bash
codex mcp add matrix-server \
  --env MATRIX_USER_ID=@you:your-homeserver.com \
  --env MATRIX_ACCESS_TOKEN=syt_... \
  --env MATRIX_HOMESERVER_URL=https://your-homeserver.com \
  --env MATRIX_DATA_DIR=/path/to/persistent/data \
  --env MATRIX_PASSWORD=your-matrix-password \
  --env MATRIX_RECOVERY_KEY='EsTc ...' \
  -- npx github:Vegard-/matrix-mcp-server
```

> **Important:** Always set `MATRIX_DATA_DIR` to an absolute path. Without it, the server defaults to `{cwd}/.data`, which may vary depending on how your MCP client launches the process — causing a fresh crypto identity on every restart.

## End-to-End Encryption

The server speaks E2EE natively. It uses a custom SQLite-backed IndexedDB adapter (`better-sqlite3`) to persist the [matrix-sdk-crypto](https://github.com/matrix-org/matrix-rust-sdk) state between restarts — no browser required.

### How it works

- **Phase 1** (automatic): Olm/Megolm crypto state is stored in `MATRIX_DATA_DIR` as a SQLite database. Device identity is stable across restarts, so other clients can trust and share keys with the server.
- **Phase 2** (optional): Cross-signing and SSSS (Secret Storage) are bootstrapped when `MATRIX_PASSWORD` is set. This enables full user identity verification and key backup.

### Enabling E2EE

Phase 1 is always on. For Phase 2, add your Matrix account password to the env:

```bash
-e MATRIX_PASSWORD=your-matrix-password
```

A recovery key is auto-generated on first run and saved to `MATRIX_DATA_DIR/ssss-recovery-key`. Keep this file safe — it's needed to restore key backup access if the crypto store is lost.

### Restoring existing encrypted history

To decrypt messages sent before this MCP device logged in, provide your existing Matrix security/recovery key from Element:

```bash
-e MATRIX_RECOVERY_KEY='EsTc ...'
```

or store it in a file and pass the file path:

```bash
-e MATRIX_RECOVERY_KEY_FILE=/path/to/matrix-recovery-key.txt
```

`MATRIX_RECOVERY_KEY` accepts the normal Matrix recovery key format shown by Element. A raw 32-byte hex key is also accepted for compatibility with `MATRIX_DATA_DIR/ssss-recovery-key`.

When a recovery key is provided, the server:

- decodes it using the Matrix cryptographic key representation
- caches the raw key at `MATRIX_DATA_DIR/ssss-recovery-key`
- uses it to unlock server-side secret storage
- loads the session backup private key from secret storage
- restores room keys from server key backup

By default, key backup restore runs automatically when a recovery key is available. To only cache/unlock the backup key and skip the full restore pass, set:

```bash
-e MATRIX_RESTORE_KEY_BACKUP=false
```

The full restore can take a long time on large accounts. Keep `MATRIX_RECOVERY_KEY`, `MATRIX_RECOVERY_KEY_FILE`, and `MATRIX_DATA_DIR/ssss-recovery-key` private.

### Known Limitations

- **Encrypted DMs may not work on all homeservers.** Direct messages (1:1 rooms) use E2EE, but some homeservers — notably Dendrite — have known issues with device key distribution and to-device message delivery that prevent Megolm key sharing between devices. Encrypted group rooms are not affected. If you experience undecryptable DMs, this is a homeserver-side limitation, not a bug in this server. Synapse-based homeservers are fully supported.

## Setup: HTTP server

For multi-user deployments or when you want a persistent endpoint. Requires cloning the repo and running the server yourself. Supports optional OAuth token exchange via an identity provider (e.g. Keycloak).

### 1. Start the server

```bash
git clone https://github.com/Vegard-/matrix-mcp-server.git
cd matrix-mcp-server
npm install && npm run build

# Configure environment
cp .env.example .env
# Edit .env with your settings (see below)

npm start
```

### 2. Environment variables

```bash
# Server
PORT=3000
CORS_ALLOWED_ORIGINS=""              # Comma-separated (empty = allow all)

# HTTPS (optional)
ENABLE_HTTPS=false
SSL_KEY_PATH="/path/to/private.key"
SSL_CERT_PATH="/path/to/certificate.crt"

# Matrix
MATRIX_HOMESERVER_URL="https://matrix.example.com"
MATRIX_DOMAIN="matrix.example.com"
MATRIX_USER_ID="@bot:matrix.example.com"
MATRIX_ACCESS_TOKEN="syt_..."
MATRIX_DATA_DIR="/var/lib/matrix-mcp/data"  # Persistent crypto store — use an absolute path
MATRIX_PASSWORD=""                           # Optional: enables cross-signing + SSSS (Phase 2 E2EE)
MATRIX_RECOVERY_KEY=""                       # Optional: existing Matrix/Element recovery key
MATRIX_RECOVERY_KEY_FILE=""                  # Optional: path to existing Matrix/Element recovery key
MATRIX_RESTORE_KEY_BACKUP=true               # Set false to skip full historical key restore

# OAuth / token exchange (optional)
ENABLE_OAUTH=false
ENABLE_TOKEN_EXCHANGE=false
IDP_ISSUER_URL="https://keycloak.example.com/realms/matrix"
IDP_AUTHORIZATION_URL="https://keycloak.example.com/realms/matrix/protocol/openid-connect/auth"
IDP_TOKEN_URL="https://keycloak.example.com/realms/matrix/protocol/openid-connect/token"
OAUTH_CALLBACK_URL="http://localhost:3000/callback"
MATRIX_CLIENT_ID="your-matrix-client-id"
MATRIX_CLIENT_SECRET="your-matrix-client-secret"
```

### 3. Connect your client

#### Claude Code

```bash
claude mcp add --scope user --transport http matrix-server http://localhost:3000/mcp \
  -H "matrix_user_id: @you:your-homeserver.com" \
  -H "matrix_homeserver_url: https://your-homeserver.com" \
  -H "matrix_access_token: syt_..."
```

#### Codex

```bash
codex mcp add matrix-server \
  --url http://localhost:3000/mcp
```

### Testing with MCP Inspector

```bash
# Start the server
npm run dev

# In another terminal, run the inspector
npx @modelcontextprotocol/inspector
```

Connect to `http://localhost:3000/mcp` to authenticate and test all available tools.

## Available Tools

### 📖 Tier 0: Read-Only Tools

#### **Room Tools**

- **`list-joined-rooms`** - Get all rooms the user has joined

  - _No parameters required_
  - Returns room names, IDs, and member counts

- **`get-room-info`** - Get detailed room information

  - `roomId` (string): Matrix room ID (e.g., `!roomid:domain.com`)
  - Returns name, topic, settings, creator, and member count

- **`get-room-members`** - List all members in a room
  - `roomId` (string): Matrix room ID
  - Returns display names and user IDs of joined members

#### **Message Tools**

- **`get-room-messages`** - Retrieve recent messages from a room

  - `roomId` (string): Matrix room ID
  - `limit` (number, default: 20): Maximum messages to retrieve
  - Returns formatted message content including text and images

- **`get-messages-by-date`** - Filter messages by date range

  - `roomId` (string): Matrix room ID
  - `startDate` (string): ISO 8601 format (e.g., `2024-01-01T00:00:00Z`)
  - `endDate` (string): ISO 8601 format
  - Returns messages within the specified timeframe

- **`identify-active-users`** - Find most active users by message count
  - `roomId` (string): Matrix room ID
  - `limit` (number, default: 10): Maximum users to return
  - Returns users ranked by message activity

#### **User Tools**

- **`get-user-profile`** - Get profile information for any user

  - `targetUserId` (string): Target user's Matrix ID (e.g., `@user:domain.com`)
  - Returns display name, avatar, presence, and shared rooms

- **`get-my-profile`** - Get your own profile information

  - _No parameters required_
  - Returns your profile, device info, and room statistics

- **`get-all-users`** - List all users known to your client
  - _No parameters required_
  - Returns display names and user IDs from client cache

#### **Search Tools**

- **`search-public-rooms`** - Discover public rooms to join
  - `searchTerm` (string, optional): Filter by name or topic
  - `server` (string, optional): Specific server to search
  - `limit` (number, default: 20): Maximum rooms to return
  - Returns room details, topics, and member counts

#### **Notification Tools**

- **`get-notification-counts`** - Check unread messages and mentions

  - `roomFilter` (string, optional): Specific room ID to check
  - Returns unread counts, mentions, and recent activity

- **`get-direct-messages`** - List all DM conversations
  - `includeEmpty` (boolean, default: false): Include DMs with no recent messages
  - Returns DM partners, last messages, and unread status

#### **Real-time Tools**

- **`wait-for-messages`** - Wait for new incoming messages in real time
  - `roomId` (string, optional): Room to watch (omit to watch all rooms including DMs)
  - `timeoutMs` (number, default: 30000): How long to wait in milliseconds
  - `since` (string, optional): Continuation token from a previous call
  - Returns messages as they arrive with a `since` token for duplicate-free follow-up calls; each message includes an `isDM` field

#### **Invite Tools**

- **`get-pending-invites`** - List rooms you've been invited to but not yet joined
  - _No parameters required_
  - Returns room names, IDs, and who sent the invite

### ✏️ Tier 1: Action Tools

#### **Messaging Tools**

- **`send-message`** - Send messages to rooms

  - `roomId` (string): Matrix room ID
  - `message` (string): Message content
  - `messageType` (enum: "text" | "html" | "emote", default: "text"): Message formatting
  - `replyToEventId` (string, optional): Event ID to reply to
  - Supports plain text, HTML formatting, and emote actions

- **`send-direct-message`** - Send private messages to users
  - `targetUserId` (string): Target user's Matrix ID
  - `message` (string): Message content
  - Automatically creates DM rooms if needed

#### **Room Management Tools**

- **`create-room`** - Create new Matrix rooms

  - `roomName` (string): Name for the new room
  - `isPrivate` (boolean, default: false): Room privacy setting
  - `topic` (string, optional): Room topic/description
  - `inviteUsers` (array, optional): User IDs to invite initially
  - `roomAlias` (string, optional): Human-readable room alias
  - Creates rooms with appropriate security settings

- **`join-room`** - Join rooms by ID or alias

  - `roomIdOrAlias` (string): Room ID or alias to join
  - Works with invitations and public rooms

- **`leave-room`** - Leave Matrix rooms

  - `roomId` (string): Room ID to leave
  - `reason` (string, optional): Reason for leaving
  - Cleanly exits rooms with optional reason

- **`invite-user`** - Invite users to rooms
  - `roomId` (string): Room to invite user to
  - `targetUserId` (string): User ID to invite
  - Respects room permissions and power levels

#### **Room Administration Tools**

- **`set-room-name`** - Update room display names

  - `roomId` (string): Room to modify
  - `roomName` (string): New room name
  - Requires appropriate room permissions

- **`set-room-topic`** - Update room topics/descriptions
  - `roomId` (string): Room to modify
  - `topic` (string): New room topic
  - Requires appropriate room permissions

#### **Message Action Tools**

- **`send-reaction`** - React to a message with an emoji
  - `roomId` (string): Matrix room ID
  - `eventId` (string): Event ID of the message to react to
  - `emoji` (string): Emoji to react with (e.g., `👍`)

- **`edit-message`** - Edit a previously sent message
  - `roomId` (string): Matrix room ID
  - `eventId` (string): Event ID of the message to edit
  - `newBody` (string): Replacement message content

- **`redact-event`** - Delete/redact a message
  - `roomId` (string): Matrix room ID
  - `eventId` (string): Event ID to redact
  - `reason` (string, optional): Reason for removal

#### **Thread Tools**

- **`get-thread-messages`** - Retrieve all messages in a thread
  - `roomId` (string): Matrix room ID
  - `threadRootEventId` (string): Event ID of the thread root
  - `limit` (number, default: 50): Maximum replies to return
  - Returns the root event plus all replies, oldest first

#### **Server Tools**

- **`restart-server`** - Hot-reload the MCP server in place
  - _No parameters required_
  - The outer process stays alive; the inner process restarts cleanly, picking up any newly built code. Use after `npm run build` to deploy changes without reconnecting.

## Development

### Available Scripts

```bash
npm run build        # Build TypeScript to dist/
npm run dev          # HTTP dev server with hot reload
npm run dev:stdio    # Stdio dev mode with hot reload
npm run start        # Production HTTP server
npm run start:stdio  # Production stdio server
npm run lint         # Run ESLint
npm run test         # Run tests
```

### Project Structure

```
src/
├── http-server.ts           # HTTP server entry point (Express)
├── stdio-server.ts          # Stdio entry point (npx / CLI)
├── server.ts               # MCP server configuration (shared)
├── tools/                  # Tool implementations
│   ├── tier0/             # Read-only tools
│   │   ├── rooms.ts       # Room information tools
│   │   ├── messages.ts    # Message retrieval tools
│   │   ├── users.ts       # User profile tools
│   │   ├── search.ts      # Room search tools
│   │   ├── notifications.ts # Notification tools
│   │   └── wait-for-messages.ts # Real-time message polling
│   └── tier1/             # Action tools
│       ├── messaging.ts   # Message sending tools
│       ├── room-management.ts # Room lifecycle tools
│       └── room-admin.ts  # Room administration tools
├── matrix/                # Matrix client management
├── utils/                 # Helper utilities
└── types/                 # TypeScript type definitions
```

## Security Considerations

- 🔒 **E2EE Native**: Reads and writes encrypted rooms using the Rust matrix-sdk-crypto engine; crypto state persists in SQLite so device identity is stable
- 🔑 **SSSS Recovery Key**: Auto-generated on first run, stored at `MATRIX_DATA_DIR/ssss-recovery-key` (mode 0600) — back this up
- 🔐 **Token Management**: Matrix clients are cached per user and reused across tool calls; access tokens stay server-side
- 🛡️ **OAuth Integration**: Prevents direct Matrix token exposure through OAuth proxy
- 🔍 **Permission Checks**: Respects Matrix room power levels and permissions
- 🚫 **Input Validation**: Comprehensive parameter validation using Zod schemas
- 🌐 **CORS Support**: Configurable origin restrictions for web clients

## Architecture

The server implements a three-layer architecture:

1. **Transport Layer**: Stdio (`stdio-server.ts`) with hot-reload outer/inner process wrapper, or HTTP (`http-server.ts`) with Express and optional OAuth
2. **MCP Layer** (`server.ts`): Tool registration and request routing (shared by both transports)
3. **Matrix Layer** (`tools/` + `src/matrix/`): Matrix homeserver communication via cached clients with persistent E2EE crypto store

The stdio server uses a **hot-reload wrapper**: the outer process (`stdio-server.ts`) manages the MCP connection and spawns an inner process that runs the actual server. When the inner process exits with code 0 (triggered by the `restart-server` tool), the outer process immediately respawns it — picking up newly built code without dropping the MCP connection.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
