# Optional Matrix raw-observation handoff

This prepares the native collector responsibility in
[PCP108](https://github.com/Just-Insane/personal-context-platform/issues/108).
It is inactive by default. It does not install, restart, configure or activate
the accepted direct connector, select a real room, grant access or add a timer.

## Retention before SDK consumption

The locked matrix-js-sdk 37.13.0 assigns next_batch before dispatching timeline
events and can continue after callback failure. Its event mapper adds room_id
and mutates event objects. A timeline callback or rendered queue receipt cannot
establish original custody or prevent a missed acquisition.

The optional MatrixClientConfig.pcpSync value binds one exact homeserver, user,
device and fixed PCP receiver URL, plus the receiver's existing ea-chat-source-v1 policy: schema,
source, account, profile, room, principal, policy_version, mode and basis.
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
No secret store, publisher process or schedule is added.

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

The receiver supplies Storage Only policy. This path grants no Search, Graph,
Attention, model use or destination processing.

### Pinned private startup input

The host must explicitly supply both `MATRIX_PCP_STARTUP_FILE` and
`MATRIX_PCP_STARTUP_SHA256`. With both absent, the loader returns immediately
without reading or discovering a policy file, and the configured client keeps
its existing ordinary arguments. A partial pair or an empty value is an error.

The file is a canonical absolute path to a service-user-owned regular file with
mode `0600`, no symbolic-link traversal, and at most 32 KiB. Its exact bytes must
match the separately supplied lowercase SHA-256 pin. The host custody of that
pin remains authoritative; changing the service-owned file cannot make new
bytes acceptable at restart. Neither input contains a credential.

Its only allowed top-level fields are `schema: "ea-matrix-pcp-startup-v1"`,
`selection`, and optionally `initialBoundary`. The selection is exactly the
existing PcpSyncSelection; the initial decision is exactly the explicit record
described below. Unknown fields, invalid native identity, a mismatched decision
or an attempted credential field reject. Parsing errors never print private
file contents or filesystem payloads.

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

Own-sender messages are retained. Encrypted payloads keep explicit decryption and
attachment-inventory gaps. Attachment locators remain references with a
missing-bytes gap; this publisher performs no decryption or attachment fetch.
Healthy parents and siblings remain independently publishable. Upstream history
is always unverified, limited windows are partial, and absent room coverage
does not imply deletion.

Invalid individual events retain blocked responsibility and exact bytes.
Ambiguous duplicate keys, invalid UTF-8, malformed selected containers,
excessive nesting and sync responses above 8 MiB fail before cursor advancement.

Edits, redactions, redacted payloads and conflicting observations remain held.
Native targets are retained, and unresolved mutations conservatively hold unsent
target publications. This does not select a current revision. Arrival order,
timestamps and receipt clocks never establish lifecycle authority.
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
