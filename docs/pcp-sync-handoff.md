# Optional Matrix raw-observation handoff

This prepares the native collector responsibility in
[PCP108](https://github.com/Just-Insane/personal-context-platform/issues/108).
It is inactive by default. It does not install, restart, configure or activate
the accepted direct connector, select a real room or grant access. An explicitly
pinned v2 startup enables the acquisition facet of the existing collector below.

## Retention before SDK consumption

The locked matrix-js-sdk 37.13.0 assigns next_batch before dispatching timeline
events and can continue after callback failure. Its event mapper adds room_id
and mutates event objects. A timeline callback or rendered queue receipt cannot
establish original custody or prevent a missed acquisition.

The optional MatrixClientConfig.pcpSync value binds one exact homeserver, user,
device and fixed PCP receiver URL. The existing ea-chat-source-v1 policy binds
one room and one mode. The ea-chat-source-v2 policy retains source, account,
profile, principal, policy_version and basis, with room_scope: all_joined and
mode: live_and_historical. It enumerates only the retained account's joined rooms;
it does not join a room or acquire a new provider grant.
The authenticated client identity is checked before its first sync.

The existing client remains the only sync client, and message-queue.db remains
its SQLite database. Within the existing request deadline, the fetch boundary
retains the selected room's exact timeline, state, state-after, invite and knock event byte
spans before returning the successful response to the SDK. No unselected room,
account-data, presence or to-device body is written to the handoff.

One SQLite transaction commits each observation, its independent responsibility,
stream/position association, explicit coverage gaps, and the matching checkpoint
in the existing sync_state.sync_token entry. The optional path uses WAL with
synchronous=FULL. A failure returns no successful response to the SDK and leaves
the previous checkpoint intact. A crash after commit preserves both observations
and checkpoint. Exact replay at that boundary deduplicates; stale or changed
replay cannot regress it. Opaque tokens are never ordered.

The optional SDK store keeps its normal in-memory room behavior and supplies
getSavedSyncToken from that same SQLite checkpoint. Setting the SDK's current
in-memory token alone does not seed its initial request. No synthetic saved-room
snapshot or second persistent cursor is introduced.

The existing rendered queue/dequeue keeps its behavior. It cannot discharge PCP
responsibility or independently advance a checkpoint once retention is bound.

## Inactive host integration

The default application supplies no pcpSync value. The optional private loader
below passes only an explicitly pinned host selection to the **existing single
client creator before startup**. It reuses that queue's pcpHandoff(selection).
No separate secret store, publisher process or host schedule is added. The v1
startup remains retention-only. The v2 startup enables a bounded timer within
the existing collector, with that collector's startup and stop lifetime.

A selection cannot be attached to an already running unbound client or replace
a different pending selection. Ordinary callers may reuse the bound client.
A database with retained responsibility cannot silently omit or replace its
selection on restart. A standalone legacy cursor cannot seed a new retention
boundary by default: binding refuses it before writing PCP tables or the
selection key. Each resumed checkpoint must match the last committed retained
batch or the separately recorded explicit initial boundary described below.
The early saved-selection check opens an existing database read-only and does
not create a missing queue or initialize schema during ordinary client login.
The existing collector's periodic SDK-token mirror remains active for ordinary
queues. When PCP owns the checkpoint, that mirror is a no-op: the SDK can lag a
committed response, and its token cannot replace durable responsibility or crash
the timer. Direct unretained checkpoint writes remain rejected.

The receiver's existing policy governs destination processing. Acquisition
configuration does not grant Search, Graph, Attention or model permissions.

### Pinned private startup input

The host must explicitly supply both `MATRIX_PCP_STARTUP_FILE` and
`MATRIX_PCP_STARTUP_SHA256`. With both absent, the loader returns immediately
without reading or discovering a policy file, and the configured client keeps
its existing ordinary arguments. A partial pair or an empty value is an error.

The file is a canonical absolute path to a service-user-owned regular file with
mode `0600`, no symbolic-link traversal, and at most 32 KiB. Its exact bytes must
match the separately supplied lowercase SHA-256 pin. The host custody of that
pin remains authoritative; changing the service-owned file cannot make new
bytes acceptable at restart. The environment pair contains only a path and pin.

Its only allowed top-level fields are `schema: "ea-matrix-pcp-startup-v1"`,
`selection`, and optionally `initialBoundary`. The selection is exactly the
existing PcpSyncSelection; the initial decision is exactly the explicit record
described below. Unknown fields, invalid native identity, a mismatched decision
or an attempted credential field in v1 reject. Parsing errors never print private
file contents or filesystem payloads.

The v2 envelope uses schema: ea-matrix-pcp-startup-v2 and adds receiverBinding
with exactly clientId and clientSecret from the existing source Access credential.
It requires the v2 all-joined selection and service principal matching clientId.
This credential remains in the same protected, pinned private startup file and
process memory; it is never written to the handoff database or public receipts.
The host obtains it through its established protected BWS custody, without
creating a credential, grant or secret-bearing command argument.

This optional loader is limited to the existing retained single-client route:
retained-read mode, the dedicated child process, and OAuth/token exchange off.
The homeserver, user and device must match its retained launch environment.
The existing configured-client helper freezes this input at module startup and
checks each requested identity before passing it to the existing client
factory. Headers cannot select a policy. A later environment/file change
cannot attach or replace a binding on an already started process.

The retained launcher's `--check` also validates explicit pinned input, using
only the same saved-session/store metadata and private configuration. It does
not open a queue, read/adopt the current cursor, create PCP tables, contact
Matrix/PCP or start a publisher. Applicability of an initial decision to the
actual checkpoint remains the transactional check at client startup.

The currently installed five-key VPS bootstrap forwards neither input. Its
existing behavior therefore stays fully inactive. A separate reviewed host
forwarding/configuration change must supply the pinned, already-approved
selection and decision. This source does not create that private file, choose
its contents, add an authorization or change the live bootstrap.

### Explicit start after an existing legacy cursor

An existing direct collector has no PCP custody for the history before its
ordinary saved cursor. The operator may separately approve starting future
selected retention there. This is a coverage decision, not evidence that the
earlier history was captured. The default path never makes that decision.

`preparePcpLegacyStart(selection, checkpoint, decisionRef)` is a pure helper:
it returns an `ea-matrix-pcp-start-v1` record containing the fixed selection's
digest, the exact opaque cursor's digest, the decision reference and
`prior_coverage: "unverified"`. It reads no database or provider, creates no
selection, and contains no raw cursor or credential. A decision reference is
audit metadata; it does not grant source access or approve its own application.

The host integration can supply this record as `MatrixClientConfig.pcpSyncStart`
alongside the separately authorized `pcpSync` selection **before creating the
existing single client**. The private loader forwards only an explicitly
supplied record; it never generates one from a cursor. A record without its
selection, a changed selection/cursor, an unexpected
field or coverage claim, a missing legacy cursor, or orphaned PCP custody is
refused. It cannot attach to a running client with a different initial decision.

After authenticating the existing native identity and before its first sync,
one immediate SQLite transaction validates the current cursor and commits the
fixed selection plus `pcp_matrix_start_boundary` in the existing `sync_state`.
It does not alter `sync_token`, copy legacy rendered messages, or invent a
retained sync batch. A transaction failure rolls back the new binding and
schema. The locked SDK reads that same existing checkpoint for its first
request; every successful new response must still retain its selected bytes,
obligations and next checkpoint before SDK delivery.

`handoff.initialBoundary()` provides exact local readback. An uncertain start
outcome is reconciled against that record and the normal checkpoint proof;
the exact already committed input is accepted without changing the cursor or
replacing responsibility, even after new batches have advanced it. Different
decisions reject. The initial record remains separate from observed batches,
and upstream history remains unverified in subsequent coverage.

Applying this decision still requires the existing runtime owner to stop the
collector watch and old provider, preserve the current state, verify absence
of the old client, and bind the exact selected policy/decision at the planned
single-client startup. This source does not enforce cross-process quiescence
against an older installed binary. The old cursor must be observed only within
that authorized activation procedure; a stale prepared digest causes refusal.
After a boundary is committed, a binary without this boundary capability is
not an eligible active fallback for that state. Preserve the source/state
receipt and use a qualified reader; do not erase the selection or rewind the
cursor to make a rollback start.

## Exact originals and useful partial results

Normal sync events often lack room_id. Their exact native wire bytes remain
retained locally with the enclosing room as provenance. The PCP envelope adds
the room explicitly, omits provider_bytes and declares **transformed** fidelity.
A wire envelope already containing the matching room is submitted as an exact
original. SDK objects are never called wire bytes.

Own-sender messages are retained. The v1 helper keeps encrypted payloads and
attachment references with explicit gaps. The v2 collector uses the existing
SDK keys and captures available native media as described below.
Healthy parents and siblings remain independently publishable. Upstream history
is always unverified, limited windows are partial, and absent room coverage
does not imply deletion.

Invalid individual events retain blocked responsibility and exact bytes.
Ambiguous duplicate keys, invalid UTF-8, malformed selected containers,
excessive nesting and sync responses above 8 MiB fail before cursor advancement.

The legacy v1 handoff holds edits, redactions and conflicting observations.
The v2 all-joined handoff publishes each native edit or redaction under its own
immutable event ID, including its source-provided target relation. It does not
replace or hold the target event. A redacted native view retains the bytes still
available with an explicit native_content_redacted gap. Genuinely conflicting
observations of the same event remain held. Arrival order, timestamps and receipt
clocks never establish authority to replace another event.
[PCP286](https://github.com/Just-Insane/personal-context-platform/issues/286)'s
authenticated native cause/currentness binding remains an unresolved interface.

## Publication and unknown outcomes

PcpReceiver binds a fixed HTTPS /v1/source/matrix-discord endpoint and existing
Access credentials to the frozen selection. Events cannot supply a URL,
principal, selector or credential. Redirects are refused. Credentials are not
written to the handoff database. The receiver URL is frozen with the selection;
an uncertain publication cannot silently move to another destination.

publishPcpObservation handles one selected obligation. Its exact observation and
compact receipt query are frozen before any remote effect. A SQLite
compare-and-set changes ready to uncertain **before** the first possible capture
POST. Only that first claim can send capture. A crash between claim and network
call may conservatively strand a never-sent item; it cannot authorize a duplicate.

Even capture HTTP 200 does not clear responsibility. reconcilePcpObservation uses
the same fixed receiver's read-only reconcile operation. Discharge requires the
existing exact positive receipt: query digest, capture/record/revision, current
representation reference, lifecycle version and submitted attachment digests,
with read_only true and replay_authorized false.

Absent, changed, unavailable, invalid or unknown results retain responsibility.
There is no capture fallback, automatic replay or public requeue operation.
The recovery owner must reconcile uncertainty through the existing receipt path.
An acknowledged parent does not clear attachment, decryption, history or lifecycle
gaps, or erase its originals. Later attachment custody, correction and erasure
remain their own existing interfaces.

## Verification and rollout boundary

### All joined rooms, full accessible history, live first

`pcp-acquisition.ts` extends the existing collector; it creates no second Matrix
client or sync consumer. Live `/sync` retains every joined room's native timeline
and state event before advancing the existing checkpoint. Invitations and
unjoined room contents are excluded. A departure preserves prior custody and
continuations; rejoining resumes them. Native room/event identities remain
separate from any claimed Discord bridge mapping.

The collector enumerates the complete native joined-room list, including quiet
rooms absent from resumed sync responses. Each room has a durable full-history
job, and a limited live window adds a separate gap job tied to the last retained
native event. Native backward `/messages` pages retain exact event spans and the
next room continuation in one SQLite transaction. History never writes
`sync_state.sync_token`. An empty page with an `end` token continues; only a
missing `end`, or the exact known event bounding a gap job, closes that job.
Repeated cursors remain stalled, and failures retain the previous continuation.
The page limit is resumable and has no total-history cutoff.

The single-flight collector turn runs every 30 seconds. It offers at most ten
publications, selecting fresh live records between remote calls. At most two
slots perform old receipt or enrichment recovery, so uncertain live work cannot
permanently crowd out history. When no live publication waits, the turn acquires
one native page of up to 100 events from a fair rotating history job. Acquisition
pauses at 200 outstanding publications and resumes as custody drains. The source
turn has a 45-second admission budget; individual reads retain their 15-second
network/body deadline. Live sync continues independently of these awaited reads.
The shared receiver retains its own live-first historical admission policy.

The existing SDK crypto context can add `observation.decrypted_event` containing
the exact room/event identity, original clear type and full original content.
It never reads a bundled replacement as the original, restores a key backup,
replaces a device or exports keys. Original ciphertext remains unchanged. Missing
keys remain explicit and are retried by the same collector after five minutes.

Available native MXC attachments are read through the authenticated homeserver,
with redirects prohibited. A publication carries up to 16 MiB of media in a
24 MiB JSON envelope; up to four native downloads are attempted per preparation.
After an exact positive receipt, a later media batch or newly available plaintext
uses the same native record and a monotonic rendition. The previous publication
and receipt are archived before a different attempt is allowed. An uncertain
publication is immutable and can only be reconciled, never blindly replayed.
Existing acknowledged media is not downloaded or resent during later batches.

Encrypted media is retained as exact ciphertext with a separate decryption gap;
it is not relabelled as a decoded file. Unsupported external locators, files over
16 MiB, missing keys, media failures, oversized native responses and conflicting
observations remain explicit gaps. Native edit/redaction events and their target
relations are published independently, including edits discovered through later
decryption of already captured ciphertext. They never rewrite their target.
Healthy siblings continue. Page traversal completion is not publication,
media, key or lifecycle completion; `coverage()` reports these outstanding work
counts separately.

For initial activation, deploy/configure the v2 receiver policy before enabling
the native publisher. A bound read-only receipt probe must show the source route
is available; starting a publisher against an absent policy would create durable
uncertainty, not permission to replay. Stop the existing collector watcher and
quiesce the exact provider before preparing the initial boundary from its current
saved cursor. Preserve the current matched session, device, crypto, queue and
private configuration. Build the reviewed immutable native revision, prepare the
private v2 startup file, and set only the existing `pcp_startup_file` and
`pcp_startup_sha256` host fields plus the release/revision pointer. Run the existing
metadata-only retained startup check, then resume the same provider and watcher.
Verify one client, saved-cursor resume, exact common receipts and durable per-room
history progress. Once the boundary is committed, rollback requires a compatible
retention-aware release and the latest state; do not erase a selection or rewind
the cursor to make an older binary start.

Focused tests use fabricated identities, payloads and credentials with temporary
real SQLite databases. They exercise the locked SDK with fake fetch responses
and a controlled fixture clock for its retained request-deadline timers,
retention failure before token assignment, callback failure after commit, exact
wire readback, restart, reordered/duplicate syncs, partial results, mismatched
receipts, durable unknown state and concurrent publication claims.

Source and fixture proof do not establish a real approved selection, runtime
hook, installed release, actual PCP capture or Dot acceptance. The single
programme rollout owner and existing Infrastructure custody owner retain those
steps. Keep the accepted direct connector running until that separately
qualified integration is ready.
