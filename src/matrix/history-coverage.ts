import type { MatrixClient, Room } from "matrix-js-sdk";

/** Preserve the existing 20-page bound, but explain every early stop. */
export async function backfillRoomHistoryUntil(
  client: Pick<MatrixClient, "scrollback">,
  room: Room,
  targetStartTs: number,
  onPage: () => void = () => {}
) {
  let batchesLoaded = 0;
  let lastRequestedCursor: string | null = null;
  const result = (stoppedBecause: "sdk_pagination_stopped" | "window_start_reached" | "no_progress" | "batch_limit") => ({
    batchesLoaded, stoppedBecause,
    nextCursor: room.oldState.paginationToken ?? lastRequestedCursor,
    nextCursorMayRepeatPage: room.oldState.paginationToken === null && lastRequestedCursor !== null,
    timelineCursorAvailable: room.oldState.paginationToken !== null,
    // SDK v37 scrollback discards `end` on empty pages. A null SDK token cannot prove exhaustion.
    endOfAccessibleHistory: null,
  });
  while (true) {
    if (room.oldState.paginationToken === null) return result("sdk_pagination_stopped");
    const events = room.getLiveTimeline().getEvents();
    const earliest = events.reduce((ts, event) => Math.min(ts, event.getTs()), Infinity);
    if (earliest <= targetStartTs) return result("window_start_reached");
    if (batchesLoaded >= 20) return result("batch_limit");
    const beforeToken = room.oldState.paginationToken;
    lastRequestedCursor = beforeToken;
    const beforeCount = events.length;
    await client.scrollback(room, 100);
    batchesLoaded += 1;
    onPage();
    if (room.oldState.paginationToken === beforeToken && room.getLiveTimeline().getEvents().length <= beforeCount) {
      return result("no_progress");
    }
  }
}
